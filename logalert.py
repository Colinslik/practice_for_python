# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import config
from eventmanager import EventDb
import logger


# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()


class LogAlert(object):
    """
    Log Alert V1
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

    def variable_extraction(self, expressions):
        variable_list = []
        lbrace = 0
        rbrace = 0
        while True:
            lbrace = expressions.find('{', lbrace)
            rbrace = expressions.find('}', rbrace)
            if lbrace is not -1:
                variable_list.append(expressions[lbrace + 1:rbrace])
                lbrace += 1
                rbrace += 1
            else:
                break
        return variable_list

    def action_parser(self, action, variable):
        _action = action
        for key, val in variable.iteritems():
            if not val:
                return None
            else:
                target = "{" + key + "}"
                replaced = '{0}'.format(val)
                _action = _action.replace(target, replaced)
        if "{" in _action or "}" in _action:
            return None
        else:
            return _action

    def dump_log(self, log_list):
        eventdb_conn = EventDb()
        for log_data in log_list:
            eventdb_conn.push_queue(log_data[4])
            eventdb_conn.db_streaming()
            log_data[4]["status"] = log_data[4]["status_failed"]
        try:
            log_filter = []
            for log_data in log_list:
                message = _conf.get(log_data[0], 'message', '')
                self.current_data = log_data
                variable_list = self.variable_extraction(message)
                if not variable_list:
                    pair_from = [{"NONE": "NONE"}]
                else:
                    pair_from = []
                for cmd_var in variable_list:
                    func = "self.get_{0}()".format(cmd_var)
                    cmd_var_list = eval(func)
                    if not cmd_var_list:
                        pair_to = [{cmd_var: None}]
                    else:
                        pair_to = []
                    for cmd_value in cmd_var_list:
                        if pair_from:
                            for every_pair_dict in pair_from:
                                temp_dict = every_pair_dict.copy()
                                temp_dict.update({cmd_var: cmd_value})
                                pair_to.append(temp_dict)
                        else:
                            pair_to.append({cmd_var: cmd_value})
                        pair_from = pair_to[:]
                for expr_dict in pair_from:
                    log = self.action_parser(message, expr_dict)
                    if log and log not in log_filter:
                        log_filter.append(log)
                log_data[4]["status"] = log_data[4]["status_succeeded"]
            for i in log_filter:
                _logger.info("[LogAlert] %s" % (i))

        except Exception:
            _logger.error("[LogAlert] Failed to dump log.")
        finally:
            eventdb_conn = EventDb()
            for log_data in log_list:
                eventdb_conn.push_queue(log_data[4])
            eventdb_conn.db_streaming()

    def get_display(self):
        return [self.current_data[2]["display"]]

    def get_host(self):
        host_vm_name = "{0} ({1})".format(
            self.current_data[1].keys()[0], self.current_data[1].values()[0]["vm_name"]) \
            if self.current_data[1].values()[0]["vm_name"] \
            else "{0}".format(self.current_data[1].keys()[0])
        return [host_vm_name]

    def get_disks(self):
        hostname = self.current_data[1].keys()[0]
        disk_status = ""
        for status in self.current_data[3][hostname].values():
            disk_status += "{0} ({1}), ".format(
                status["disk_name"], status["near_failure"])
        return [disk_status[:-2]]


if __name__ == '__main__':
    pass
