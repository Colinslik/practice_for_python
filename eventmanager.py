# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from datetime import datetime
from httplib import OK, CREATED, NOT_FOUND
import platform
import time

import config
import logger
import pdaclient


# Get configuration and logger
_conf = config.get_config()
_logger = logger.get_logger()


class Event_Manager(object):

    def __init__(self):
        global _conf
        _conf = config.get_config()

        self.event_list = []

    def regist_events(self):
        event_dict = {}
        for eventid in _conf.sections():
            if eventid.startswith("event"):
                event_dict.update({eventid: {}})
                for (key, val) in _conf.items(eventid):
                    event_dict[eventid].update({key: val})
        for key, val in event_dict.iteritems():
            event_id = key
            display = val["display"]
            if "severity" in val:
                severity = val["severity"]
            else:
                severity = "Medium"
            conditional = val["conditional"]
            self.event_list.append(
                Event(event_id, display, severity, conditional))

    def get_event_list(self):
        return self.event_list


class Event(object):

    def __init__(self, _event_id=None,
                 _display=None,
                 _severity=None,
                 _conditional=None,
                 _triggered=False):
        self.event_id = _event_id
        self.display = _display
        self.severity = _severity
        self.conditional = _conditional
        self.triggered = _triggered

    def get_event_id(self):
        return self.event_id

    def get_display(self):
        return self.display

    def get_severity(self):
        return self.severity

    def get_conditional(self):
        return self.conditional

    def set_triggered(self, _triggered):
        self.triggered = _triggered

    def get_triggered(self):
        return self.triggered

    def variable_extraction(self):
        variable_list = []
        lbrace = 0
        rbrace = 0
        while True:
            lbrace = self.get_conditional().find('{', lbrace)
            rbrace = self.get_conditional().find('}', rbrace)
            if lbrace is not -1:
                variable_list.append(self.get_conditional()[lbrace + 1:rbrace])
                lbrace += 1
                rbrace += 1
            else:
                break
        return variable_list

    def condition_parser(self, variable):
        condition = self.get_conditional()
        for key, val in variable.iteritems():
            target = "{" + key + "}"
            replaced = '"{0}"'.format(val)
            condition = condition.replace(target, replaced)
        return condition


class EventDb(object):

    def __init__(self):
        self._eventqueue = []

    def db_streaming(self):
        for i in self._eventqueue:
            self.send_eventdb(i["job_id"], i["pda_server"], i["policy"],
                              i["eventname"], i["hostname"], i["vm_name"],
                              i["diskname"], i["severity"], i["status"],
                              i["action"], i["action_result"], i["reason"])
        self._eventqueue = []

    def send_eventdb(self, job_id, pda_server, policy, eventname,
                     hostname, vm_name, diskname, severity, status,
                     action, action_result, reason):
        field_dict = {"job_id": job_id,
                      "pda_server": pda_server,
                      "event": eventname,
                      "host_name": hostname,
                      "vm_name": vm_name,
                      "disk_name": diskname,
                      "severity": severity,
                      "policy": policy,
                      "action": action,
                      "status": status,
                      "action_result": action_result,
                      "reason": reason}

        eventdb_conn = EventDBTool()
        eventdb_conn.setup_cmd(field_dict)

    def push_queue(self, event_dict):
        self._eventqueue.append(event_dict)

    def get_queue(self):
        return self._eventqueue


class EventDBTool(pdaclient.PdaApi):
    """
    PDA EVENTDB Tool V1 API
    """

    def __init__(self):
        super(EventDBTool, self).__init__()
        self.tablename = "pda_jobs"

    def query_job_id_lastest(self):
        try:
            url = "%s/%s/jobs?columns='',max(job_id),''" % (
                self.endpoint, platform.node())

            params = {}

            status, data, _ = self._send_cmd(
                'GET', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.error(
                    "event db accesss fail. status: %s, results: %s"
                    % (status, data))
                raise Exception(data)

            job_id = int(data["jobs"][0]["max"])
            return job_id

        except Exception:
            return 0

    def query_event_lastest(self, filter_dict):
        try:
            url = "%s/%s/jobs?" % (
                self.endpoint, platform.node())

            if 'host_name' in filter_dict:
                url = url + 'host=%s&' % (filter_dict['host_name'])
            if 'policy' in filter_dict:
                url = url + 'policy=%s&' % (filter_dict['policy'])
            if 'event' in filter_dict:
                url = url + 'event=%s&' % (filter_dict['event'])

            url = url + 'limit=1'

            params = {}

            status, data, _ = self._send_cmd(
                'GET', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.error(
                    "event db accesss fail. status: %s, results: %s"
                    % (status, data))
                raise Exception(data)

            timestamp = int(time.mktime(datetime.strptime(
                data["jobs"][0]["time"], "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))

            event_dict = {timestamp: {}}
            target = event_dict[timestamp]
            for key, value in data["jobs"][0].iteritems():
                if key != 'time':
                    if isinstance(value, int):
                        target.update({str(key): int(value)})
                    elif isinstance(value, long):
                        target.update({str(key): float(value)})
                    else:
                        target.update({str(key): str(value)})
            return event_dict

        except Exception:
            return None

    def query_event_by_interval(self, time_interval, filter_dict):
        try:
            url = "%s/%s/jobs?" % (
                self.endpoint, platform.node())

            if 'host_name' in filter_dict:
                url = url + 'host=%s&' % (filter_dict['host_name'])
            if 'policy' in filter_dict:
                url = url + 'policy=%s&' % (filter_dict['policy'])
            if 'event' in filter_dict:
                url = url + 'event=%s&' % (filter_dict['event'])
            if time_interval:
                url = url + \
                    'tsfrom="%s"&tsto="%s"&' % (
                        datetime.utcfromtimestamp(
                            time_interval[0]).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        datetime.utcfromtimestamp(
                            time_interval[1]).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

            url = url + 'limit=1'

            url = url.replace('"', '%22').replace(' ', '%20')

            params = {}

            status, data, _ = self._send_cmd(
                'GET', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.error(
                    "event db accesss fail. status: %s, results: %s"
                    % (status, data))
                raise Exception(data)

            timestamp = int(time.mktime(datetime.strptime(
                data["jobs"][0]["time"], "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))

            event_dict = {timestamp: {}}
            target = event_dict[timestamp]
            for key, value in data["jobs"][0].iteritems():
                if key != 'time':
                    if isinstance(value, int):
                        target.update({str(key): int(value)})
                    elif isinstance(value, long):
                        target.update({str(key): float(value)})
                    else:
                        target.update({str(key): str(value)})
            return event_dict

        except Exception:
            return None

    def setup_cmd(self, field_dict):
        try:
            url = "%s/%s/jobs?" % (
                self.endpoint, platform.node())

            params = {"events": [field_dict]}

            status, data, _ = self._send_cmd(
                'PUT', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.debug("Write Failed. The status code is %s" %
                              (status))
                raise Exception(data)

            else:
                _logger.debug("Write Completed.")

        except Exception:
            raise


if __name__ == '__main__':
    eventdb = EventDBTool()
    eventdb.setup_cmd({
        "action": "action.noop",
        "action_result": "original schedule: [* */8 * * *], status: active new schedule: [* */2 * * *], status: active",
        "disk_name": "DELL-2",
        "event": "event.dp.predict.diskfailure.critical",
        "host_name": "WIN-A4AQ4781FJH",
        "job_id": 15,
        "pda_server": "DESKTOP-6I3D1JB",
        "policy": "policy.drprophet.schedule.diskfailure.critical",
        "reason": "[WIN-A4AQ4781FJH][disk 1(Good), disk 0(Good), disk 3(Good), disk 2(Good), DELL-0(Good), DELL-1(Good), DELL-2(Good), disk 4(Good)] Event: policy.drprophet.schedule.diskfailure.critical  ended",
        "severity": "Critical",
        "status": "ended",
        "vm_name": ""
    })
    print eventdb.query_job_id_lastest()
    print eventdb.query_event_lastest({"policy": "policy.drprophet.schedule.diskfailure.critical",
                                       "event": "event.dp.predict.diskfailure.critical",
                                       "host_name": "WIN-A4AQ4781FJH"})
    end_time = int(time.time())
    start_time = end_time - 86400
    print eventdb.query_event_by_interval([start_time, end_time], {"policy": "policy.drprophet.schedule.diskfailure.critical",
                                                                   "event": "event.dp.predict.diskfailure.critical",
                                                                   "host_name": "WIN-A4AQ4781FJH"})
