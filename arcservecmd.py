# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import multiprocessing
import threading

import config
from eventmanager import EventDb
import logger
import windowsclient


# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()


class ArcserveApi(object):
    """
    Arcserve connector V1 API
    """

    def __init__(self, mode="cmd"):
        global _conf
        _conf = config.get_config()

        if "ps" == mode:
            self.run_mode = self.setup_ps
            self.mode_name = "Arcserve PowerShell"
        else:
            self.run_mode = self.setup_cmd
            self.mode_name = "Arcserve Command"
        self.host = _conf.get('service.arcserve', 'host', '')
        self.user = _conf.get('service.arcserve', 'username', '')
        self.password = _conf.get('service.arcserve', 'password', '')
        self.timeout = _conf.getint('main', 'main.action.timeout', 300)
        self.windows_conn = windowsclient.WindowsApi(
            self.host, self.user, self.password)
#        self.host_ip_pair = {}

    def run(self, cmd):
        status_code, std_out, std_err = self.run_mode(cmd)
        return status_code, std_out, std_err

    def setup_cmd(self, cmd):
        status_code, std_out, std_err = self.windows_conn.setup_cmd(cmd)
        return status_code, std_out, std_err

    def setup_ps(self, ps_script):
        status_code, std_out, std_err = self.windows_conn.setup_ps(ps_script)
        return status_code, std_out, std_err

    def thread_monitor(self, func, cmd, waiting_time=None,
                       event_data=None):
        try:
            thread = threading.Thread(target=func, args=(
                cmd, event_data), name="ArcserveAction")
            thread.start()
            if waiting_time:
                thread_timeout = waiting_time + 10
            else:
                thread_timeout = 60
            thread.join(thread_timeout)
            if thread.is_alive():
                _logger.error("[Timeout] %s" % (cmd))
                if event_data:
                    event_data["action_result"] = "[Timeout] {0}".format(
                        cmd).replace('"', '\\"')
                    eventdb_conn = EventDb()
                    eventdb_conn.push_queue(event_data)
                    eventdb_conn.db_streaming()
        except Exception as e:
            _logger.error("[Exception] Invalid Command: %s" % (cmd))
            _logger.debug("%s" % (e))
            if event_data:
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(event_data)
                eventdb_conn.db_streaming()

    def setup_thread(self, cmd, event_data=None):
        try:
            status_code, std_out, std_err = self.run(cmd)
            if not std_err:
                action_result = "Completed"
                if event_data:
                    event_data["status"] = event_data["status_succeeded"]
            else:
                action_result = "Failed"
            _logger.info("[%s] %s" % (action_result, cmd))
            action_return = ""
            if std_out:
                _logger.info("%s" % (std_out))
                action_return = std_out + "\n"
            if std_err:
                _logger.warning("%s" % (std_err))
                action_return += std_err
            if event_data:
                event_data["action_result"] = action_return.replace('"', '\\"')
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(event_data)
                eventdb_conn.db_streaming()
            return status_code, std_out, std_err

        except Exception as e:
            _logger.error("[Exception] Invalid Command: %s" % (cmd))
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

    def action_parser(self, action, variable):
        _action = action
        for key, val in variable.iteritems():
            if not val:
                return None
            else:
                target = "{" + key + "}"
                replaced = '{0}'.format(val)
                _action = _action.replace(target, replaced)
        _logger.debug("[Acrserve Debug] Command: %s" % (_action))
        if "{" in _action or "}" in _action:
            return None
        else:
            return _action

    def run_thread(self, func, cmd, waiting_time,
                   event_data=None):
        process = multiprocessing.Process(target=self.thread_monitor, args=(
            func, cmd, waiting_time, event_data), name="ThreadMonitor")
        process.start()
        return process

    def run_cmd(self, action_list):
        if not self.host or not self.user or not self.password:
            if action_list:
                _logger.warning(
                    "[AcrserveCommand] Server info is not sufficient.")
                _logger.debug(
                    "[AcrserveCommand] host:%s user:%s" % (self.host, self.user))
            return None
        elif action_list:
            eventdb_conn = EventDb()
            for act_data in action_list:
                eventdb_conn.push_queue(act_data[4])
                eventdb_conn.db_streaming()
                act_data[4]["status"] = act_data[4]["status_failed"]
        else:
            return None
        cmd_list = []
        timeout_list = []
        event_list = []
        process_list = []
        for act_data in action_list:
            try:
                command = _conf.get(act_data[0], 'command', '')
                timeout = _conf.getint(act_data[0], 'timeout', self.timeout)
                host_vm_name = "{0} ({1})".format(
                    act_data[1].keys()[0], act_data[1].values()[0]["vm_name"]) \
                    if act_data[1].values()[0]["vm_name"] \
                    else "{0}".format(act_data[1].keys()[0])
                self.current_host = act_data[1]
                variable_list = self.variable_extraction(command)
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
                                if temp_dict not in pair_to:
                                    pair_to.append(temp_dict)
                        else:
                            pair_to.append({cmd_var: cmd_value})
                        pair_from = pair_to[:]
                for expr_dict in pair_from:
                    cmd = self.action_parser(command, expr_dict)
                    if cmd:
                        cmd_list.append(cmd)
                        timeout_list.append(timeout)
                        event_list.append(act_data[4])
                        _logger.info("[%s] Run %s: %s" %
                                     (host_vm_name,
                                      self.mode_name, cmd))
                    else:
                        eventdb_conn = EventDb()
                        eventdb_conn.push_queue(act_data[4])
                        eventdb_conn.db_streaming()
                        _logger.error("[%s] Failed to run %s: %s. Please check whether arguments are enough or not." %
                                      (host_vm_name,
                                       self.mode_name, command))
            except Exception:
                eventdb_conn = EventDb()
                eventdb_conn.push_queue(act_data[4])
                eventdb_conn.db_streaming()
                _logger.error("[%s] Failed to run %s: %s. Argument is not found." %
                              (host_vm_name,
                               self.mode_name, command))
        for idx, val in enumerate(cmd_list):
            process_list.append(
                self.run_thread(self.setup_thread, val, timeout_list[idx],
                                event_list[idx]))
        for proc in process_list:
            proc.join()

    def get_cmdpath(self):
        if not hasattr(self, 'cmdpath') or not self.cmdpath:
            self.cmdpath = ['"{0}"'.format(
                _conf.get('service.arcserve', 'cmdpath', ''))]
        return self.cmdpath

    def get_job_description_fullbackup(self):
        if not hasattr(self, 'job_description_fullbackup') or \
                not self.job_description_fullbackup:
            self.job_description_fullbackup = ['"{0}"'.format(_conf.get(
                'service.arcserve', 'job_description_fullbackup', ''))]
        return self.job_description_fullbackup

    def get_job_description_backupjob(self):
        if not hasattr(self, 'job_description_backupjob') or \
                not self.job_description_backupjob:
            self.job_description_backupjob = ['"{0}"'.format(_conf.get(
                'service.arcserve', 'job_description_backupjob', ''))]
        return self.job_description_backupjob

    def get_wait_time(self):
        if not hasattr(self, 'wait_time') or not self.wait_time:
            self.wait_time = ['{0}'.format(
                _conf.get('service.arcserve', 'wait_time', ''))]
        return self.wait_time

    def get_agent_username(self):
        if not hasattr(self, 'agent_username') or not self.agent_username:
            self.agent_username = ['{0}'.format(_conf.get(
                'service.arcserve', 'agent_username', ''))]
        if not self.agent_username:
            _logger.error(
                "[AcrserveCommand] Username can not be empty.")
        return self.agent_username

    def get_agent_password(self):
        if not hasattr(self, 'agent_password') or not self.agent_password:
            self.agent_password = ['{0}'.format(_conf.get(
                'service.arcserve', 'agent_password', ''))]
        if not self.agent_password:
            _logger.error(
                "[AcrserveCommand] Password can not be empty.")
        return self.agent_password

    def get_host(self):
     #       hostname = self.current_host.keys()[0]
     #       if hostname not in self.host_ip_pair:
     #           self.host_ip_pair.update({hostname: []})
     #           for i in self.current_host[hostname]["host_ip"]:
     #               if self.windows_conn.connection_test(i):
     #                   self.host_ip_pair[hostname] = [i]
     #                   break
     #       return self.host_ip_pair[hostname]
        return self.current_host.keys()

    def get_job_no(self):
        job_no_list = []
        filtered_list = []
        cmd = 'cd {0} & ca_qmgr -list'.format(self.get_cmdpath()[0])
        result = self.run(cmd)[1]
        for line in result.splitlines():
            if self.get_job_description_backupjob()[0][1:-1] in line:
                job_no_list.append(line.split()[0])
        for i in job_no_list:
            log_result = self.run(
                'cd {0} & ca_qmgr -view {1}'.format(
                    self.get_cmdpath()[0], i))[1]
            pos_start = log_result.find("Source Targets")
            pos_end = log_result.find("Destination Target")
            if log_result[pos_start:pos_end] and \
                    self.get_host()[0] in log_result[pos_start:pos_end]:
                filtered_list.append(i)
            elif not log_result[pos_start:pos_end] \
                    and self.get_host()[0] in log_result:
                filtered_list.append(i)

        return filtered_list

    def get_arcserve_info(self, host_watchdog):
        len_dict = []
        info_list = []
        jobinfo_dict = {}
        try:
            cmd = 'cd {0} & ca_qmgr -list'.format(self.get_cmdpath()[0])
            result = self.run(cmd)[1]
            for line in result.splitlines():
                try:
                    line.split()[1]
                except Exception:
                    continue
                else:
                    if not len_dict:
                        for key in line.split():
                            pos = line.find(key)
                            if (pos + len(key)) == len(line):
                                len_dict.append({key: None})
                            else:
                                len_dict.append({key: pos + len(key)})
                    else:
                        idx = 0
                        job_dict = {}
                        for field in len_dict:
                            job_dict[field.keys()[0]] = line[idx:field.values()[
                                0]].lstrip()
                            idx = field.values()[0]
                        info_list.append(job_dict)

            for host in host_watchdog:
                if host not in jobinfo_dict:
                    jobinfo_dict[host] = []

            for entry in info_list:
                log_result = self.run(
                    'cd {0} & ca_qmgr -view {1}'.format(
                        self.get_cmdpath()[0], entry["JOB#"]))[1]
                pos_start = log_result.find("Source Targets")
                pos_end = log_result.find("Destination Target")
                for host in host_watchdog:
                    if log_result[pos_start:pos_end] and \
                            host in log_result[pos_start:pos_end]:
                        jobinfo_dict[host].append(entry)
                    elif not log_result[pos_start:pos_end] \
                            and host in log_result:
                        jobinfo_dict[host].append(entry)
        except Exception as e:
            _logger.warning(
                "[AcrserveInfo] Job info failed to update.")
            _logger.debug(
                "%s" % (e))
        _logger.debug("Arcserve Jobs: %s" % (jobinfo_dict))
        return jobinfo_dict


if __name__ == '__main__':
    import time
    test1 = ArcserveApi()
    test2 = ArcserveApi("ps")
#    test1.run_thread(
#        test1.run, 'cd {0} & ca_backup -source 172.31.8.155 -tape TAPE1 -description "Job Created By Predictive Data Adapter" -runjobnow'.format(test1.get_cmdpath()[0]), 60)
#    test1.run_thread(
#        test1.run, 'cd {0} & ca_backup -source 172.31.8.155 -rotation -mediapool testpool -jobunit full overwrite -jobunit incr overwrite -jobunit incr append -jobunit incr append -jobunit incr append -jobunit incr append -jobunit incr append -on 12/12/2018 -at 00:00 -description "Job Created By Predictive Data Adapter" -username administrator -password password'.format(test1.get_cmdpath()[0]), 60)
#    test1.run_thread(
# test1.run, 'cd {0} & echo y | ca_qmgr -delete
# "4"'.format(test1.get_cmdpath()[0]), 60)
    test1.run_thread(
        test1.setup_thread, 'cd {0} & ca_qmgr -view 5'.format(test1.get_cmdpath()[0]), 30)
    test1.run_thread(
        test1.setup_thread, 'cd {0} & ca_qmgr -list'.format(test1.get_cmdpath()[0]), 30)
    print test1.run(
        'cd {0} & ca_qmgr -list'.format(test1.get_cmdpath()[0]))[1]
    print test2.run("""$strComputer = $Host
                            Clear
                            $RAM = WmiObject Win32_ComputerSystem
                            $MB = 1048576
    "Installed Memory: " + [int]($RAM.TotalPhysicalMemory /$MB) + " MB" """)[1]
    time.sleep(60)
