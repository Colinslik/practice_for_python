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


class RawCmdApi(object):
    """
    Raw Command connector V1 API
    """

    def __init__(self, mode="cmd"):
        global _conf
        _conf = config.get_config()

        if "ps" == mode:
            self.run_mode = self.setup_ps
            self.mode_name = "Raw PowerShell"
        else:
            self.run_mode = self.setup_cmd
            self.mode_name = "Raw Command"
        self.host = _conf.get('service.arcserve', 'host', '')
        self.user = _conf.get('service.arcserve', 'username', '')
        self.password = _conf.get('service.arcserve', 'password', '')
        self.timeout = _conf.getint('main', 'main.action.timeout', 300)
        self.windows_conn = windowsclient.WindowsApi(
            self.host, self.user, self.password)

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
                cmd, event_data), name="RawAction")
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
                    event_data["status"] = event_data["status_change"]
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

    def run_thread(self, func, cmd, waiting_time,
                   event_data=None):
        process = multiprocessing.Process(target=self.thread_monitor, args=(
            func, cmd, waiting_time, event_data), name="ThreadMonitor")
        process.start()
        return process

    def run_cmd(self, action_list):
        if not self.host or not self.user or not self.password:
            if action_list:
                _logger.warning("[RawCommand] Server info is not sufficient.")
                _logger.debug(
                    "[RawCommand] host:%s user:%s" % (self.host, self.user))
                eventdb_conn = EventDb()
                for act_data in action_list:
                    eventdb_conn.push_queue(act_data[4])
                eventdb_conn.db_streaming()
            return None
        cmd_list = []
        timeout_list = []
        event_list = []
        process_list = []
        for act_data in action_list:
            command = _conf.get(act_data[0], 'command', '')
            timeout = _conf.getint(act_data[0], 'timeout', self.timeout)
            self.current_host = act_data[1]
            cmd_list.append(command)
            timeout_list.append(timeout)
            event_list.append(act_data[4])
            _logger.info("[%s] Run %s: %s" %
                         (self.current_host.keys()[0],
                          self.mode_name,  command))
        for idx, val in enumerate(cmd_list):
            process_list.append(
                self.run_thread(self.setup_thread, val, timeout_list[idx],
                                event_list[idx]))
        for proc in process_list:
            proc.join()

    def noop_action(self, action_list):
        eventdb_conn = EventDb()
        for act_data in action_list:
            act_data[4]["status"] = act_data[4]["status_change"]
            eventdb_conn.push_queue(act_data[4])
        eventdb_conn.db_streaming()


if __name__ == '__main__':
    import time
    test1 = RawCmdApi()
    test1.run_thread(
        test1.setup_thread, 'dir / w', 30)
    test2 = RawCmdApi("ps")
    test2.run_thread(
        test2.setup_thread, """$strComputer = $Host
                            Clear
                            $RAM = WmiObject Win32_ComputerSystem
                            $MB = 1048576
    "Installed Memory: " + [int]($RAM.TotalPhysicalMemory /$MB) + " MB" """, 30)
    test2.run_thread(
        test2.setup_thread, 'get-process', 30)
    time.sleep(60)
