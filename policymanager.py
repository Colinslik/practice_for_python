#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import socket
import time

import config
import eventmanager
import logger


# Get configuration and logger
_conf = config.get_config()
_logger = logger.get_logger()


class PolicyManager(object):

    def __init__(self, _eventqueue):
        global _conf
        _conf = config.get_config()

        self.eventqueue = _eventqueue
        self.policy_list = []

    def regist_policies(self):
        policy_dict = {}

        for i in _conf.sections():
            if i.startswith("policy"):
                policy_dict.update({i: {}})
                for (key, val) in _conf.items(i):
                    if key == "enabled":
                        policy_dict[i].update(
                            {key: _conf.getboolean(i, key, "")})
                    elif key == "suppression":
                        policy_dict[i].update({key: _conf.getint(i, key, "")})
                    else:
                        policy_dict[i].update({key: val})

        _logger.debug("policy list: %s" % (policy_dict))
        for key, val in policy_dict.iteritems():
            if val["enabled"] and val["type"] == "every-time":
                suppression = val["suppression"]
                event_id = val["event"]
                conditional = '{event}'
                actions = [x.strip() for x in val["actions"].split(',')]
                self.policy_list.append(
                    Policy(suppression, None, None,
                           event_id, conditional, actions, key))
            elif val["enabled"] and val["type"] == "start-end":
                event_id = val["event"]
                status_trigger = [{"status": "ended"},
                                  {"failed_status": "ended_failed"}]
                status_change = {"status": "started"}
                conditional = '{event}'
                actions = [x.strip() for x in val["actions.start"].split(',')]
                self.policy_list.append(
                    Policy(0, status_trigger, status_change,
                           event_id, conditional, actions, key))
                status_trigger = [{"status": "started"},
                                  {"failed_status": "ended_failed"}]
                status_change = {"status": "ended"}
                conditional = 'not {event}'
                actions = [x.strip() for x in val["actions.end"].split(',')]
                self.policy_list.append(
                    Policy(0, status_trigger, status_change,
                           event_id, conditional, actions, key))

    def policy_trigger(self):
        action_list = []
        self.regist_policies()
        for policy in self.policy_list:
            for event in self.eventqueue:
                if policy.get_event_id() == event[0].get_event_id() and\
                        self.get_trigger(policy, event[0]):
                    target_host = event[1].keys()[0]
                    _logger.debug("action name: %s   policy event id: %s   event name: %s"
                                  "   policy conditional: %s   event status: %s" % (
                                      policy.get_actions(),
                                      policy.get_event_id(),
                                      event[0].get_event_id(),
                                      policy.get_conditional(),
                                      event[0].get_triggered()))
                    if self.check_event_db(
                            policy.get_event_id(),
                            target_host,
                            policy.get_suppression(),
                            policy.get_status_trigger(),
                            policy.get_policy_id()):
                        self.current_job_id = self.job_id
                        host_vm_name = "{0} ({1})".format(
                            target_host, event[2]["vm_name"]) \
                            if event[2]["vm_name"] \
                            else "{0}".format(target_host)
                        disk_name = ""
                        disk_status = ""
                        actions = ""
                        reason = ""
                        for disk_list in event[1].values():
                            for disk, status in disk_list.iteritems():
                                # disk_name += "{0}\\n".format(
                                #    status["disk_name"])
                                disk_name += "{0}, ".format(
                                    status["disk_name"])
                                disk_status += \
                                    "{0}({1}), ".format(
                                        disk, status["near_failure"])
                        disk_info_list = self.get_last_diskname()
                        if not disk_info_list:
                            disk_info_list = disk_name[:-2]
                        reason = "[{0}][{1}] Event: {2}  {3}".format(
                            host_vm_name,
                            disk_status[:-2],
                            policy.get_policy_id(),
                            self.get_current_event_status()[:-1])
                        _logger.info("[%s][%s] Event: %s  %s" % (
                            host_vm_name, disk_info_list.replace("\\n", ", "),
                            policy.get_policy_id(),
                            self.get_current_event_status()[:-1]))
                        for action in policy.get_actions():
                            actions += "{0}, ".format(action)

                            action_list.append(
                                {self.get_priority():
                                 [action,
                                  {target_host:
                                   event[2]},
                                  {
                                      "display": event[0].get_display(),
                                      "policy_name": policy.get_policy_id(),
                                      "policy_status":
                                          self.get_current_event_status()[:-1]
                                          if self.get_current_event_status()
                                          else "triggered"
                                  },
                                    event[1], {
                                      "job_id": self.get_job_id(),
                                      "pda_server": self.get_pdahost(),
                                      "policy": policy.get_policy_id(),
                                      "eventname": policy.get_event_id(),
                                      "hostname": target_host,
                                      "vm_name": "" if not event[2]["vm_name"]
                                      else event[2]["vm_name"],
                                      "diskname": disk_info_list,
                                      "severity": event[0].get_severity(),
                                      "status": '{0}processing'.format(
                                          self.get_current_event_status()),
                                      "action": action,
                                      "action_result":
                                      self.get_last_action_resule(),
                                      "reason": reason,
                                      "status_succeeded":
                                      self.get_event_status_change(),
                                      "status_failed": '{0}failed'.format(
                                          self.get_current_event_status())}]})

                        _logger.debug("[%s][%s][%s] trigger time: %s"
                                      % (host_vm_name,
                                         disk_status[:-2], actions[:-2],
                                         int(time.time() * 1000000000)))

        return action_list

    def get_trigger(self, policy, event):
        replaced = {"event": event.get_triggered()}
        check_conditional = policy.get_conditional().format(**replaced)
        return eval(check_conditional)

    def get_priority(self):
        return self.priority

    def get_last_action_resule(self):
        return self.action_result

    def get_current_event_status(self):
        return self.current_event_status

    def get_last_diskname(self):
        return self.last_diskname

    def get_event_status_change(self):
        return self.event_status_change

    def check_event_db(self, eventname, hostname,
                       suppression, status_trigger, policy):
        self.priority = 1
        self.current_event_status = ''
        self.event_status_change = 'completed'
        self.last_diskname = ''
        self.action_result = ''
        self.job_id = self.get_current_job_id() + 1
        filter_dict = {"pda_server": self.get_pdahost()}
        if status_trigger:
            filter_dict.update({"policy": policy,
                                "event": eventname,
                                "host_name": hostname})
            result = self.query_eventdb(filter_dict, suppression)

            _status_trigger = ""
            for and_cond in status_trigger:
                _status_trigger += '('
                for key1, val1 in and_cond.iteritems():
                    _status_trigger += '{' + key1 + '}' + \
                        ' == "{0}" and '.format(val1)
                _status_trigger = _status_trigger[:-5] + ') or '
            if result:
                for key, val in result.iteritems():
                    if "processing" in val["status"]:
                        now = int(time.time())
                        if now - key < 43200:
                            return False
                        else:
                            replaced = {"status": '"ended"',
                                        "failed_status": '""'}
                            self.current_event_status = 'started_'
                            self.event_status_change = 'started'
                            self.priority = 2
                    elif "started_failed" == val["status"]:
                        self.current_event_status = 'started_'
                        self.event_status_change = 'started'
                        # replaced = {"status": '"ended"',
                        #            "failed_status": '"started_failed"'}
                        replaced = {"status": '"ended"',
                                    "failed_status": '""'}
                        if "job_id" in val:
                            self.job_id = val["job_id"]
                        self.priority = 2
                    elif "started" == val["status"]:
                        self.current_event_status = 'ended_'
                        self.event_status_change = 'ended'
                        replaced = {"status": '"started"',
                                    "failed_status": '""'}
                        self.last_diskname = val["disk_name"]
                        self.action_result = val["action_result"] \
                            if "action_result" in val else ''
                        if "job_id" in val:
                            self.job_id = val["job_id"]
                        self.priority = 3
                    elif "ended_failed" == val["status"]:
                        # end_failed retry
                        if '"started"' in _status_trigger:
                            self.current_event_status = 'ended_'
                            self.event_status_change = 'ended'
                            # replaced = {"status": '"started"',
                            #            "failed_status": '"ended_failed"'}
                            replaced = {"status": '"ended_failed"',
                                        "failed_status": '""'}
                            self.last_diskname = val["disk_name"]
                            self.action_result = val["action_result"] \
                                if "action_result" in val else ''
                            if "job_id" in val:
                                self.job_id = val["job_id"]
                            self.priority = 3
                        # retire this action and reset status to ended to run a
                        # new round.
                        else:
                            self.current_event_status = 'started_'
                            self.event_status_change = 'started'
                            # replaced = {"status": '"ended"',
                            #            "failed_status": '"ended_failed"'}
                            replaced = {"status": '"ended"',
                                        "failed_status": '""'}
                            self.priority = 2
                    elif "ended" == val["status"]:
                        self.current_event_status = 'started_'
                        self.event_status_change = 'started'
                        replaced = {"status": '"ended"',
                                    "failed_status": '""'}
                        self.priority = 2
                    else:
                        replaced = {"status": '"ended"',
                                    "failed_status": '""'}
                        self.current_event_status = 'started_'
                        self.event_status_change = 'started'
                        self.priority = 2
            else:
                replaced = {"status": '"ended"',
                            "failed_status": '""'}
                self.current_event_status = 'started_'
                self.event_status_change = 'started'
                self.priority = 2
            try:
                check_conditional = _status_trigger[:-4].format(**replaced)
            except Exception:
                return False
            else:
                return eval(check_conditional)
        else:
            filter_dict.update({"policy": policy,
                                "event": eventname,
                                "host_name": hostname})
            result = self.query_eventdb(filter_dict, suppression)
            if result:
                for val in result.values():
                    if "job_id" in val:
                        self.job_id = val["job_id"]
                    if "failed" != val["status"]:
                        return False
        return True

    def query_eventdb(self, filter_dict, suppression):
        eventdb_conn = eventmanager.EventDBTool()
        if suppression != 0:
            end_time = int(time.time())
            start_time = end_time - suppression
            result = eventdb_conn.query_event_by_interval(
                [start_time, end_time], filter_dict)
        else:
            result = eventdb_conn.query_event_lastest(filter_dict)
        return result

    def get_pdahost(self):
        if not hasattr(self, 'pdahost') or not self.pdahost:
            self.pdahost = socket.gethostname()
        return self.pdahost

    def get_current_job_id(self):
        if not hasattr(self, 'current_job_id') or not self.current_job_id:
            eventdb_conn = eventmanager.EventDBTool()
            self.current_job_id = eventdb_conn.query_job_id_lastest()
        return self.current_job_id

    def get_job_id(self):
        if not hasattr(self, 'job_id') or not self.job_id:
            self.job_id = self.get_current_job_id()
        return self.job_id


class Policy(object):

    def __init__(self, _suppression=None,
                 _status_trigger=None,
                 _status_change=None,
                 _event_id=None,
                 _conditional=None,
                 _actions=None,
                 _policy_id=None):
        self.suppression = _suppression
        self.status_trigger = _status_trigger
        self.status_change = _status_change
        self.event_id = _event_id
        self.conditional = _conditional
        self.actions = _actions
        self.policy_id = _policy_id

    def get_suppression(self):
        return self.suppression

    def get_status_trigger(self):
        return self.status_trigger

    def get_status_change(self):
        return self.status_change

    def get_event_id(self):
        return self.event_id

    def get_conditional(self):
        return self.conditional

    def get_actions(self):
        return self.actions

    def get_policy_id(self):
        return self.policy_id

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


if __name__ == '__main__':
    pass
