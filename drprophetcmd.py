# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import multiprocessing
import threading

import config
import drpclient
from eventmanager import EventDb
import logger

# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()


class DRProphetApi(object):
    """
    DRProphet connector V1 API
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

        try:
            self.auth = _conf.get('service.drprophet', 'auth', '')
            if self.auth:
                self.drp_conn = drpclient.DRPAPIClient(self.auth)
        except Exception:
            _logger.error("Cannot get the API endpoint for DRProphet.")
        self.timeout = _conf.getint('main', 'main.action.timeout', 300)
        self.host_id_pair = {}

    def run(self, cmd):
        status_code, response = eval(cmd.values()[0])
        return status_code, response

    def thread_monitor(self, func, cmd, waiting_time=None,
                       event_data=None):
        try:
            thread = threading.Thread(target=func, args=(
                cmd, event_data), name="DRProphetAction")
            thread.start()
            if waiting_time:
                thread_timeout = waiting_time + 10
            else:
                thread_timeout = 60
            thread.join(thread_timeout)
            if thread.is_alive():
                _logger.error("[Timeout] %s" % (cmd.keys()[0]))
                if event_data:
                    event_data["action_result"] = "[Timeout] {0}".format(
                        cmd.keys()[0]).replace('"', '\\"')
                    eventdb_conn = EventDb()
                    eventdb_conn.push_queue(event_data)
                    eventdb_conn.db_streaming()
        except Exception as e:
            _logger.error("[Exception] Invalid Command: %s" % (cmd.keys()[0]))
            _logger.debug("%s" % (e))
            if event_data:
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(event_data)
                eventdb_conn.db_streaming()

    def setup_thread(self, cmd, event_data=None):
        try:
            status_code, response = self.run(cmd)
            if status_code:
                action_result = "Completed"
                if event_data:
                    event_data["status"] = event_data["status_succeeded"]
            else:
                action_result = "Failed"
            _logger.info("[%s] %s" % (action_result, cmd.keys()[0]))
            action_return = ""
            if response:
                _logger.info("%s" % (response))
                action_return = response
            if event_data:
                event_data["action_result"] = action_return.replace('"', '\\"')
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(event_data)
                eventdb_conn.db_streaming()
            return status_code, response

        except Exception as e:
            _logger.error("[Exception] Invalid Command: %s" % (cmd.keys()[0]))
            _logger.debug("%s" % (e))
            if event_data:
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(event_data)
                eventdb_conn.db_streaming()

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

    def run_thread(self, func, cmd, waiting_time,
                   event_data=None):
        process = multiprocessing.Process(target=self.thread_monitor, args=(
            func, cmd, waiting_time, event_data), name="ThreadMonitor")
        process.start()
        return process

    def run_cmd(self, action_list):
        if not self.drp_conn.drp_login():
            if action_list:
                _logger.warning(
                    "[DRProphetCommand] Server info is not sufficient.")
            return None
        self.get_hostid()
        for act_data in action_list:
            command = _conf.get(act_data[0], 'command', '')
            timeout = _conf.getint(act_data[0], 'timeout', self.timeout)
            host_vm_name = "{0} ({1})".format(
                act_data[1].keys()[0], act_data[1].values()[0]["vm_name"]) \
                if act_data[1].values()[0]["vm_name"] \
                else "{0}".format(act_data[1].keys()[0])
            # if ( is physical and not protected) or ( is vm and no vm name )
            # or ( is vm and not protected )
            if (act_data[1].keys()[0].lower() not in self.host_id_pair) and \
                (not act_data[1].values()[0]["vm_name"] or
                 act_data[1].values()[0]["vm_name"].lower()
                 not in self.host_id_pair):
                skip_log = ("[{0}] has been skipped "
                            "because of not protected "
                            "in DRProphet.".format
                            (host_vm_name))
                _logger.info("%s" % (skip_log))
                eventdb_conn = EventDb()
                act_data[4]["status"] = act_data[4]["status_succeeded"]
                act_data[4]["action_result"] = "{0}".format(
                    skip_log).replace('"', '\\"')
                eventdb_conn.push_queue(act_data[4])
                eventdb_conn.db_streaming()
                continue
            else:
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(act_data[4])
                eventdb_conn.db_streaming()
                act_data[4]["status"] = act_data[4]["status_failed"]
            self.current_host = act_data[1]
            self.current_action = act_data[0]
            self.action_result = act_data[4]["action_result"]
            variable_list = self.variable_extraction(command)
            if variable_list:
                for cmd_var in variable_list:
                    func = "self.get_{0}()".format(cmd_var)
                    api_name = "{0} {1}".format(
                        host_vm_name, cmd_var)
                    api_caller = eval(func)
                    _logger.info("[%s] API Caller: Run %s action." %
                                 (host_vm_name, cmd_var))

                    proc = self.run_thread(self.setup_thread,
                                           {api_name: api_caller}, timeout,
                                           act_data[4])
                    proc.join()

    def get_hostid(self):
        status, result = self.drp_conn.get_protection_list()
        if not status:
            _logger.error("[Exception] Fail to get protection info.")
            return False

        for protection_info in result.values():
            self.host_id_pair.update(
                {str(protection_info["name"].lower()):
                 str(protection_info["id"])})
        return True

    def get_fullbackup(self):
        if self.current_host.keys()[0].lower() in self.host_id_pair:
            host_id = self.host_id_pair[self.current_host.keys()[0].lower()]
        else:
            host_id = self.host_id_pair[
                self.current_host.values()[0]["vm_name"].lower()]
        api_caller = 'self.drp_conn.get_create_snapshot("{0}")'.format(
            host_id)
        return api_caller

    def get_backupjob(self):
        if self.current_host.keys()[0].lower() in self.host_id_pair:
            host_id = self.host_id_pair[self.current_host.keys()[0].lower()]
        else:
            host_id = self.host_id_pair[
                self.current_host.values()[0]["vm_name"].lower()]
        schedule = _conf.get(self.current_action, 'schedule', '0 0 * * *')
        if self.auth == 'api':
            sched_status = "activate"
            api_caller = 'self.drp_conn.v1_update_schedule("{0}", "{1}", "{2}")' \
                .format(host_id, schedule, sched_status)
        else:
            api_caller = 'self.drp_conn.get_add_schedule("{0}", "{1}")'.format(
                host_id, schedule)
        return api_caller

    def get_removejob(self):
        if self.current_host.keys()[0].lower() in self.host_id_pair:
            host_id = self.host_id_pair[self.current_host.keys()[0].lower()]
        else:
            host_id = self.host_id_pair[
                self.current_host.values()[0]["vm_name"].lower()]

        if self.auth == 'api':
            if self.action_result and "original schedule" in self.action_result:
                left_pos = self.action_result.find("[")
                right_pos = self.action_result.find("]")
                schedule = self.action_result[left_pos + 1:right_pos]
                left_status_pos = self.action_result.find("status: ")
                right_status_pos = self.action_result.find("\nnew")
                sched_status = self.action_result[left_status_pos +
                                                  8:right_status_pos]
            else:
                schedule = None
                sched_status = "suspended"
            api_caller = 'self.drp_conn.v1_update_schedule("{0}", "{1}", "{2}")' \
                .format(host_id, schedule, sched_status)
        else:
            api_caller = 'self.drp_conn.get_delete_schedule("{0}")'.format(
                host_id)
        return api_caller

    def crontab_parser(self, crontab):
        schedule_vals = crontab.split()
        schedule_type = ""

        if '*/' in schedule_vals[0] and schedule_vals[1] == '*' \
                and schedule_vals[2] == '*' and schedule_vals[3] == '*' \
                and schedule_vals[4] == '*':
            pos = schedule_vals[0].find("/")
            schedule_type = "every {0} minute(s)".format(
                int(schedule_vals[0][pos + 1:]))
        elif schedule_vals[0] == '*' and '*/' in schedule_vals[1] \
                and schedule_vals[2] == '*' and schedule_vals[3] == '*' \
                and schedule_vals[4] == '*':
            pos = schedule_vals[1].find("/")
            schedule_type = "every {0} hour(s)".format(
                int(schedule_vals[1][pos + 1:]))
        elif schedule_vals[0] == '*' and schedule_vals[1] == '*' \
                and '*/' in schedule_vals[2] and schedule_vals[3] == '*' \
                and schedule_vals[4] == '*':
            pos = schedule_vals[2].find("/")
            schedule_type = "every {0} day(s)".format(
                int(schedule_vals[2][pos + 1:]))
        else:
            '''
            if '*' not in schedule_vals[0] and schedule_vals[1] == '*' \
                    and schedule_vals[2] == '*' and schedule_vals[3] == '*' \
                    and schedule_vals[4] == '*':
                schedule_type = "HOURLY "
            elif '*' not in schedule_vals[0] and '*' not in schedule_vals[1] \
                    and schedule_vals[2] == '*' and schedule_vals[3] == '*' \
                    and schedule_vals[4] == '*':
                schedule_type = "DAILY "
            elif '*' not in schedule_vals[0] and '*' not in schedule_vals[1] \
                    and schedule_vals[2] == '*' and schedule_vals[3] == '*' \
                    and '*' not in schedule_vals[4]:
                schedule_type = "WEEKLY "
            elif '*' not in schedule_vals[0] and '*' not in schedule_vals[1] \
                    and '*' not in schedule_vals[2] and schedule_vals[3] == '*' \
                    and schedule_vals[4] == '*':
                schedule_type = "MONTHLY "
            else:
                schedule_type = "YEARLY "
            '''
            minute_str = ""
            hour_str = ""
            week_str = ""
            day_str = ""
            month_str = ""
            for idx, val in enumerate(schedule_vals):
                if '*' not in val and idx != 4:
                    str = ""
                    for num in val.split(','):
                        if int(num) % 10 == 1:
                            str += '1st, '
                        elif int(num) % 10 == 2:
                            str += '2nd, '
                        elif int(num) % 10 == 3:
                            str += '3rd, '
                        else:
                            str += '{0}th, '.format(num)
                    if idx == 0:
                        minute_str = " {0} minute".format(str[:-2])
                    elif idx == 1:
                        hour_str = " {0} hour,".format(str[:-2])
                    elif idx == 2:
                        day_str = " of {0} day".format(str[:-2])
                    elif idx == 3:
                        month_str = " of {0} month".format(str[:-2])
                elif '*' not in val and idx == 4:
                    str = ""
                    for num in val.split(','):
                        if int(num) == 0:
                            str += 'Sun, '
                        elif int(num) == 1:
                            str += 'Mon, '
                        elif int(num) == 2:
                            str += 'Tue, '
                        elif int(num) == 3:
                            str += 'Wed, '
                        elif int(num) == 4:
                            str += 'Thu, '
                        elif int(num) == 5:
                            str += 'Fri, '
                        elif int(num) == 6:
                            str += 'Sat, '
                    week_str = " of {0}".format(str[:-2])

            schedule_type += "{0}{1}{2}{3}{4}".format(
                hour_str, minute_str, day_str, month_str, week_str)

        return schedule_type

    def get_drprophet_info(self, host_watchdog, vm_name_dict):
        from datetime import datetime
        import croniter
        schedinfo_dict = {}
        if not self.auth:
            raise ValueError(
                "Cannot get auth setting of %s" % 'DRProphet')
        try:
            if not self.get_hostid():
                raise ValueError(
                    "Cannot get the API result for %s" % 'DRProphet')
            sched_status, sched_result = self.drp_conn.get_schedule_list()
            if not sched_status:
                raise ValueError(
                    "Cannot get the API result for %s" % 'DRProphet')

            if self.auth == 'password' or self.auth == 'rsa':
                pda_sched_status, pda_sched_result = \
                    self.drp_conn.crontab_schedule_list()
                if not pda_sched_status:
                    raise ValueError(
                        "Cannot get the Crontab result for %s" % 'DRProphet')

            for hostname in host_watchdog:
                schedinfo_dict.update({hostname: []})

            for hostname, info in schedinfo_dict.iteritems():
                Server = ""
                if hostname.lower() in vm_name_dict and \
                        vm_name_dict[hostname.lower()] in self.host_id_pair:
                    node_id = self.host_id_pair[vm_name_dict[hostname.lower()]]
                else:
                    node_id = hostname

                for sched_info in sched_result.values():
                    if node_id in sched_info["resource"]:
                        status, result = self.drp_conn.get_protection_detail(
                            node_id)
                        if status:
                            Server = str(result["storage_svr"])

                        pos = sched_info["description"].rfind(" ")
                        job_type = sched_info["description"][pos + 1:]
                        description = "{0}-schdule-{1}".format(
                            hostname, job_type)
                        job_status = str(sched_info["status"])
                        job_no = str(sched_info["id"])
                        owner = self.drp_conn.user
                        crontab_info = self.crontab_parser(
                            sched_info["schedule"])
                        exec_time = str(
                            datetime.utcfromtimestamp(int(
                                sched_info["updated_at"])).strftime(
                                    '%m/%d/%Y %H:%M:%S'))
                        info.append({
                            "STATUS": job_status,
                            "LAST-RESULT": "",
                            "DESCRIPTION": description,
                            "EXEC-TIME": exec_time,
                            "JOBID": "",
                            "JOB#": job_no,
                            "JOB-TYPE": job_type,
                            "OWNER": owner,
                            "EXECUTIONHOST": Server,
                            "INFO": crontab_info})

                if self.auth == 'password' or self.auth == 'rsa':
                    for line in pda_sched_result.splitlines():
                        if node_id in line:
                            pos = line.rfind(" curl")
                            cron_raw = line[:pos]
                            now = datetime.now()
                            cron = croniter.croniter(cron_raw, now)
                            exec_time = str(
                                datetime.utcfromtimestamp(int(
                                    cron.get_next(float))).strftime(
                                        '%m/%d/%Y %H:%M:%S'))
                            crontab_info = self.crontab_parser(cron_raw)
                            pos = line.rfind(" #")
                            info.append({
                                "STATUS": "active",
                                "LAST-RESULT": "",
                                "DESCRIPTION": line[pos + 2:],
                                "EXEC-TIME": exec_time,
                                "JOBID": "",
                                "JOB#": "0",
                                "JOB-TYPE": "snapshot",
                                "OWNER": "root",
                                "EXECUTIONHOST": Server,
                                "INFO": crontab_info})
        except Exception as e:
            _logger.warning(
                "[DRProphetInfo] Schedule info failed to update.")
            _logger.debug(
                "%s" % (e))
        _logger.debug("DRProphet schedules: %s" % (schedinfo_dict))
        return schedinfo_dict


if __name__ == '__main__':
    pass
