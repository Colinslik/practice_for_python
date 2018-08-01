# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import config
import dpclient
import logger


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
                              i["eventname"], i["hostname"], i["diskname"],
                              i["severity"], i["status"], i["action"],
                              i["action_result"], i["reason"])

    def send_eventdb(self, job_id, pda_server, policy, eventname,
                     hostname, diskname, severity, status, action,
                     action_result, reason):
        tag_dict = {"pda_server": pda_server}
        field_dict = {"job_id": job_id,
                      "event": eventname,
                      "host_name": hostname,
                      "disk_name": diskname,
                      "severity": severity,
                      "policy": policy,
                      "action": action,
                      "status": status,
                      "action_result": action_result,
                      "reason": reason}

        eventdb_conn = EventDBTool()
        eventdb_conn.setup_cmd(tag_dict, field_dict)

    def push_queue(self, event_dict):
        self._eventqueue.append(event_dict)

    def get_queue(self):
        return self._eventqueue


class EventDBTool(dpclient.InfluxdbApi):
    """
    InfluxDB Tool V1 API
    """

    def __init__(self):
        super(EventDBTool, self).__init__()
        self.tablename = "pda_jobs"

    def query_job_id_lastest(self, filter_dict):
        try:
            self.set_display("max(job_id)")
            result = self.query_data_detail_setup(self.tablename,
                                                  None,
                                                  filter_dict, None, None,
                                                  None, True, True)
        except dpclient.DbError:
            return 0

        else:
            job_id = int(result["max"][0][0])
            return job_id

    def query_event_lastest(self, filter_dict):
        event_dict = {}
        try:
            result = self.query_data_detail_setup(self.tablename,
                                                  None,
                                                  filter_dict, None, None,
                                                  1, True, True)
        except dpclient.DbError:
            return None

        else:
            key_list = []
            for key in result.keys():
                if "time" != key:
                    key_list.append(key)
            for i in range(len(result["time"])):
                event_dict.update({result["time"][i][0]: {}})
                target = event_dict[result["time"][i][0]]
                for j in key_list:
                    target.update({j: result[j][i]})
            return event_dict

    def query_event_by_interval(self, time_interval, filter_dict):
        event_dict = {}
        try:
            result = self.query_data_detail_setup(self.tablename,
                                                  time_interval,
                                                  filter_dict, None, None,
                                                  1, True, True)
        except dpclient.DbError:
            return None

        else:
            key_list = []
            for key in result.keys():
                if "time" != key:
                    key_list.append(key)
            for i in range(len(result["time"])):
                event_dict.update({result["time"][i][0]: {}})
                target = event_dict[result["time"][i][0]]
                for j in key_list:
                    target.update({j: result[j][i]})
            return event_dict

    def setup_cmd(self, tag_dict, field_dict):

        tag_str = ""
        field_str = ""
        for key, val in tag_dict.iteritems():
            tag_str += ",{0}={1}".format(key, val)

        for key, val in field_dict.iteritems():
            if isinstance(val, bool):
                field_str += '{0}={1},'.format(key, val)
            elif isinstance(val, int):
                field_str += '{0}={1}i,'.format(key, val)
            else:
                field_str += '{0}="{1}",'.format(key, val)

        params = '{0}{1} {2}'.format(
            self.tablename, tag_str, field_str[:-1])

        self._post_cmd(params)


if __name__ == '__main__':
    pass
