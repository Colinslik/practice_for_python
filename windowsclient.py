# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import os
import shlex
import subprocess

import winrm
import logger

# Get logger
_logger = logger.get_logger()


class WindowsApi(object):
    """
    Windows connector V1 API
    """

    def __init__(self, hostname=None, user=None, passwd=None):
        if hostname and user and passwd:
            self.host = hostname
            self._sendcmd = self._setup_cmd_remote
            self._sendps = self._setup_ps_remote
            self._connection_test = self._connection_test_remote
            self.user = user
            self.passwd = passwd
        else:
            self.host = "localhost"
            self._sendcmd = self._setup_cmd_local
            self._sendps = self._setup_ps_local
            self._connection_test = self._connection_test_local

    def connection_test(self, ip):
        return self._connection_test(ip)

    def setup_cmd(self, cmd):
        return self._sendcmd(cmd)

    def setup_ps(self, ps_script):
        return self._sendps(ps_script)

    def _setup_cmd_remote(self, cmd):
        try:
            session = winrm.Session(self.host, auth=(self.user, self.passwd))
            result = session.run_cmd(cmd)
        except Exception as e:
            _logger.error("WinRM error happened.")
            _logger.debug("%s" % (e))
            raise
        return result.status_code, result.std_out, result.std_err

    def _setup_cmd_local(self, cmd):
        try:
            status_code, std_out, std_err = self.timeout_command(cmd)
        except Exception as e:
            _logger.error("Subprocess error happened.")
            _logger.debug("%s" % (e))
            raise
        return status_code, std_out, std_err

    def _setup_ps_remote(self, ps_script):
        try:
            session = winrm.Session(self.host, auth=(self.user, self.passwd))
            result = session.run_ps(ps_script)
        except Exception as e:
            _logger.error("WinRM error happened.")
            _logger.debug("%s" % (e))
            raise
        return result.status_code, result.std_out, result.std_err

    def _setup_ps_local(self, ps_script):
        try:
            process = subprocess.Popen(["powershell", ps_script],
                                       stdout=subprocess.PIPE).communicate()
        except Exception as e:
            _logger.error("Subprocess error happened.")
            _logger.debug("%s" % (e))
            raise
        if process[1]:
            return 1, process[0], process[1]
        else:
            return 0, process[0], process[1]

    def timeout_command(self, command):
        """ call shell-command and either return its output or kill it
            if it doesn't normally exit within timeout seconds and return None
        """
        args = shlex.split(command)
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            startupinfo=startupinfo,
            universal_newlines=True,
            shell=True).communicate()
        if process[1]:
            return 1, process[0].decode('string_escape'), process[1].decode('string_escape')
        else:
            return 0, process[0].decode('string_escape'), process[1].decode('string_escape')

    def _connection_test_remote(self, ip):
        return self._connection_test_local(ip)

    def _connection_test_local(self, ip):
        ret = self.setup_cmd("ping {0} -n 1".format(ip))[1]
        if "TTL=" in ret:
            return True
        else:
            return False


if __name__ == '__main__':
    test1 = WindowsApi()
    test2 = WindowsApi("172.31.7.88", "administrator", "abc=123")
    print test1.connection_test("172.31.7.16")
    print test2.connection_test("172.31.6.17")
    print test1.setup_cmd("cd / & dir / w")[1]
    print test2.setup_cmd("ipconfig -all")[1]
    print test1.setup_ps("""$strComputer = $Host
                            Clear
                            $RAM = WmiObject Win32_ComputerSystem
                            $MB = 1048576
    "Installed Memory: " + [int]($RAM.TotalPhysicalMemory /$MB) + " MB" """)[1]
    print test2.setup_ps("get-process")[1]
