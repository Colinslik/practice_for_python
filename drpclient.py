# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2017 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import StringIO
from httplib import NOT_FOUND, BAD_REQUEST
from httplib import OK, CREATED, NO_CONTENT, ACCEPTED
import json
import traceback

import paramiko
import requests
import urllib3

import config
import logger


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()

success_status = [OK, CREATED, NO_CONTENT, ACCEPTED]


class DRPAPIClient(object):
    """
    DRProphet V1 API client
    """

    def __init__(self, auth="password"):
        # reload configuration
        global _conf
        _conf = config.get_config()

        self.auth = auth
        self._get_endpoint()

    def _send_cmd(self, method, url, header, params,
                  expected_status):
        """
        send API command
        """

        _logger.debug("drpclient: URL(%s) method(%s) header(%s) body(%s)" % (
            url, method, header, params))
        # send request

        try:
            if method == "POST":
                result = requests.post(
                    url, data=json.dumps(params), headers=header, verify=False)
            elif method == "DELETE":
                result = requests.delete(
                    url, headers=header, verify=False)
            elif method == "PATCH":
                result = requests.patch(
                    url, data=json.dumps(params), headers=header, verify=False)
            else:
                result = requests.get(
                    url, headers=header, verify=False)
        except Exception:
            _logger.warning(
                "drpclient: %s %s returns response %s (expects: %s)" %
                (method, url, BAD_REQUEST, str(success_status)))
            return BAD_REQUEST, None
        else:
            if result.status_code in expected_status:
                if result.text:
                    return result.status_code, json.loads(result.text)
                else:
                    return result.status_code, result.text
            else:
                return BAD_REQUEST, None

    def _get_endpoint(self):
        if self.auth == "password" or self.auth == "rsa":
            self.host = _conf.get('service.drprophet', 'host', '')
            self.user = _conf.get('service.drprophet', 'username', '')
            if self.auth == "password":
                self.passwd = _conf.get('service.drprophet', 'password', '')

                if not all([self.host, self.user, self.passwd]):
                    raise ValueError(
                        "Cannot get the SSH endpoint for %s" % 'DRProphet')
            else:
                self.key = _conf.get('service.drprophet', 'key', '')

                if not all([self.host, self.user, self.key]):
                    raise ValueError(
                        "Cannot get the SSH endpoint for %s" % 'DRProphet')

            self._drp_login = self.ssh_login
            self._protection_list = self.ssh_protection_list
            self._protection_detail = self.ssh_protection_detail
            self._schedule_list = self.ssh_schedule_list
            self._create_snapshot = self.ssh_create_snapshot
            self._add_schedule = self.ssh_add_schedule
            self._delete_schedule = self.ssh_delete_schedule
        else:
            self.host = _conf.get('service.drprophet', 'host', '')
            self.user = _conf.get('service.drprophet', 'username', '')
            self.passwd = _conf.get('service.drprophet', 'password', '')

            if not all([self.host, self.user, self.passwd]):
                raise ValueError(
                    "Cannot get the API endpoint for %s" % 'DRProphet')

            self.url = "https://%s:8352/v1" % self.host
            self.cookie = ""
            self._drp_login = self.v1_login
            self._protection_list = self.v1_protection_list
            self._protection_detail = self.v1_protection_detail
            self._schedule_list = self.v1_schedule_list
            self._create_snapshot = self.v1_create_snapshot
            self._add_schedule = self.v1_add_schedule
            self._delete_schedule = self.v1_delete_schedule

    def drp_login(self):
        return self._drp_login()

    def get_protection_list(self):
        status, data = self._protection_list()
        return status, data

    def get_protection_detail(self, hostname):
        status, data = self._protection_detail(hostname)
        return status, data

    def get_schedule_list(self):
        status, data = self._schedule_list()
        return status, data

    def get_create_snapshot(self, hostname):
        status, data = self._create_snapshot(hostname)
        return status, data

    def get_add_schedule(self, hostname, schedule):
        status, data = self._add_schedule(hostname, schedule)
        return status, data

    def get_delete_schedule(self, hostname):
        status, data = self._delete_schedule(hostname)
        return status, data

    def v1_login(self):
        # curl -X POST "<ROOTURL>/users/login.php?username=drprophet&password=password"
        # {"success":true,"msg":"OK"}
        url = "%s/sessions/" % self.url

        header = {
            'Content-Type': 'application/json'
        }

        params = {
            'user': self.user,
            'password': self.passwd
        }

        status, data = self._send_cmd(
            'POST', url, header, params, success_status)
        if status in success_status and data:
            # FIXME: save cookie for next command
            self.cookie = str(data["token"])
        else:
            _logger.error(
                "Authentication Error (host= %s, user= %s)" %
                (self.host, self.user))
            return False

        return True

    def v1_protection_list(self):
        result = False
        protection = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        url = "%s/protection/nodes/" % (self.url)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        status, data = self._send_cmd(
            'GET', url, header, None, success_status)
        if status in success_status:
            result = True
            protection = data
        else:
            _logger.error("Fail to get protection list.")

        return result, protection

    def v1_protection_detail(self, hostname):
        result = False
        protection = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        url = "%s/protection/nodes/%s" % (self.url, hostname)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        status, data = self._send_cmd(
            'GET', url, header, None, success_status)
        if status in success_status:
            result = True
            protection = data
        else:
            _logger.error("Fail to get protection detail. id: %s." %
                          (hostname))

        return result, protection

    def v1_schedule_list(self):
        result = False
        schedules = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        url = "%s/schedule/" % (self.url)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        status, data = self._send_cmd(
            'GET', url, header, None, success_status)
        if status in success_status:
            result = True
            schedules = data
        else:
            _logger.error("Fail to get schedule list.")

        return result, schedules

    def v1_create_snapshot(self, hostname):
        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        url = "%s/protection/nodes/%s/snapshots/" % (self.url, hostname)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        params = {
            'name': "{0}-snapshot".format(hostname),
        }

        status, data = self._send_cmd(
            'POST', url, header, params, success_status)
        if status in success_status:
            return True, None
        else:
            _logger.error("Fail to create snapshot. hostname: %s" % (hostname))
            return False, None

    def v1_add_schedule(self, hostname, schedule):
        result = False
        schedules = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        url = "%s/schedule/" % (self.url)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        params = {
            'resource': '/protection/nodes/{0}/snapshots/'.format(hostname),
            'action': 'post',
            'schedule': schedule,
            'params': {}
        }

        status, data = self._send_cmd(
            'POST', url, header, params, success_status)
        if status in success_status:
            result = True
            schedules = data
        else:
            _logger.error("Fail to add schedule. (hostname: %s, schedule: %s)"
                          % (hostname, schedule))

        return result, schedules

    def v1_delete_schedule(self, hostname):
        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        status, data = self.v1_schedule_list()
        if not status:
            return False, None

        for key, info in data.iteritems():
            if hostname in info["resource"]:
                schedule_id = int(key)
                break

        url = "%s/schedule/%d" % (self.url, schedule_id)

        header = {
            'X-Auth-Token': self.cookie,
            'Content-Type': 'application/json'
        }

        status, data = self._send_cmd(
            'DELETE', url, header, None, success_status)
        if status in success_status:
            return True, None
        else:
            _logger.error("Fail to delete schedule. hostname: %s"
                          % (hostname))
            return False, None

    def v1_update_schedule(self, hostname, schedule, schedule_status):
        result = False
        ori_sched = ""
        new_sched = ""
        ori_status = "suspended"
        new_status = schedule_status
        schedule_id = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        status, data = self.v1_schedule_list()
        if not status:
            return False, None

        for key, info in data.iteritems():
            if hostname in info["resource"]:
                schedule_id = int(key)
                ori_sched = info["schedule"]
                ori_status = info["status"]
                break

        if not schedule:
            retcode, data = self.v1_delete_schedule(hostname)
            if retcode:
                status = OK
                new_status = "suspended"
            else:
                status = NOT_FOUND
                new_sched = ori_sched
                new_status = ori_status
        else:
            if not schedule_id:
                retcode, data = self.v1_add_schedule(hostname, schedule)
                if retcode:
                    if schedule_status == "activate":
                        retcode, data = self.v1_activate_schedule(hostname)
                    else:
                        retcode, data = self.v1_suspend_schedule(hostname)

                    if retcode:
                        status = OK
                    else:
                        status = NOT_FOUND
                else:
                    status = NOT_FOUND
            else:
                url = "%s/schedule/%d" % (self.url, schedule_id)

                header = {
                    'X-Auth-Token': self.cookie,
                    'Content-Type': 'application/json'
                }

                params = {
                    'resource': '/protection/nodes/{0}/snapshots/'.format(
                        hostname),
                    'action': 'post',
                    'schedule': schedule,
                    'params': {}
                }

                retcode, data = self._send_cmd(
                    'PATCH', url, header, params, success_status)

                if retcode in success_status:
                    if schedule_status == "activate":
                        retcode, data = self.v1_activate_schedule(hostname)
                    else:
                        retcode, data = self.v1_suspend_schedule(hostname)

                    if retcode:
                        status = OK
                    else:
                        status = NOT_FOUND
                else:
                    status = NOT_FOUND

            if status in success_status:
                result = True
                new_sched = data["schedule"]
                new_status = data["status"]
            else:
                new_sched = ori_sched
                new_status = ori_status
                _logger.error(
                    "Fail to update schedule. (hostname: %s, schedule: %s)"
                    % (hostname, schedule))

        schedules = ('original schedule: [{0}], status: {1}\n'
                     'new schedule: [{2}], status: {3}').format(
            ori_sched, ori_status, new_sched, new_status)
        return result, schedules

    def v1_activate_schedule(self, hostname):
        schedule_id = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        status, data = self.v1_schedule_list()
        if not status:
            return False, None

        for key, info in data.iteritems():
            if hostname in info["resource"]:
                schedule_id = int(key)
                break

        if not schedule_id:
            status = NOT_FOUND
        else:
            url = "%s/schedule/%d/activate" % (self.url, schedule_id)

            header = {
                'X-Auth-Token': self.cookie,
                'Content-Type': 'application/json'
            }

            status, data = self._send_cmd(
                'POST', url, header, None, success_status)

        if status in success_status:
            return True, data
        else:
            _logger.error("Fail to activate schedule. hostname: %s"
                          % (hostname))
            return False, None

    def v1_suspend_schedule(self, hostname):
        schedule_id = None

        if not self.v1_login():
            _logger.error("Fail to login into DRProphet.")
            return False, None

        status, data = self.v1_schedule_list()
        if not status:
            return False, None

        for key, info in data.iteritems():
            if hostname in info["resource"]:
                schedule_id = int(key)
                break

        if not schedule_id:
            status = NOT_FOUND
        else:
            url = "%s/schedule/%d/suspend" % (self.url, schedule_id)

            header = {
                'X-Auth-Token': self.cookie,
                'Content-Type': 'application/json'
            }

            status, data = self._send_cmd(
                'POST', url, header, None, success_status)

        if status in success_status:
            return True, data
        else:
            _logger.error("Fail to suspend schedule. hostname: %s"
                          % (hostname))
            return False, None

    def ssh_login(self):
        cmd = 'echo "Connection successful."'
        try:
            status, data = self._send_ssh_cmd(cmd)
            if status:
                return True
        except Exception:
            _logger.error(
                "Authentication Error (host= %s, user= %s)" %
                (self.host, self.user))
        return False

    def ssh_protection_list(self):
        protection_info = {}
        try:
            cmd = (('sqlite3 -line '
                    '/opt/prophetstor/drprophet/var/db/infodata.db'
                    ' "select * from API_Node_View"'))

            status, data = self._send_ssh_cmd(cmd)
            if not status:
                _logger.error("Fail to get protection list.")
                return False, {}

            data_list = []
            data_dict = {}
            for line in data.split("\n"):
                if line:
                    pos = line.find(" = ")
                    data_dict.update(
                        {line[:pos].lstrip(): line[pos + 3:].lstrip()})
                else:
                    data_list.append(data_dict)
                    data_dict = {}

            for info in data_list:
                if info["ID"] not in protection_info:
                    protection_info.update({info["ID"]:
                                            {"id": info["ID"],
                                             "name": info["Name"],
                                             "machine_type":
                                             info["Machine_Type"]}})

            cmd = (('sqlite3 -line '
                    '/opt/prophetstor/drprophet/var/db/infodata.db'
                    ' "select * from NodeMapInfoTable"'))

            status, data = self._send_ssh_cmd(cmd)
            if not status:
                _logger.error("Fail to get protection list.")
                return False, {}

            data_list = []
            data_dict = {}
            for line in data.split("\n"):
                if line:
                    pos = line.find(" = ")
                    data_dict.update(
                        {line[:pos].lstrip(): line[pos + 3:].lstrip()})
                else:
                    data_list.append(data_dict)
                    data_dict = {}

            for info in data_list:
                if info["CA_Hostname"] in protection_info:
                    target = protection_info[info["CA_Hostname"]]
                    target.update({"storage_svr":
                                   "/storage_svrs/{0}".format(
                                       info["SA_Hostname"])})

        except Exception:
            return False, {}

        return True, protection_info

    def ssh_protection_detail(self, hostname):
        ret_status = False
        protection_info = {}
        status, data = self.ssh_protection_list()
        if not status:
            _logger.error("Fail to get protection info of %s." % (hostname))
            return False, {}

        if hostname in data:
            protection_info.update(data[hostname])
            ret_status = True

        return ret_status, protection_info

    def ssh_schedule_list(self):
        schedule_info = {}
        try:
            cmd = (('sqlite3 -line '
                    '/opt/prophetstor/drprophet/var/db/infodata.db'
                    ' "select * from ScheduleInfoTable"'))

            status, data = self._send_ssh_cmd(cmd)
            if not status:
                _logger.error(
                    "Fail to list schedule.")
                return False, {}

            data_list = []
            data_dict = {}
            merge_key = ""
            merge_str = ""
            for line in data.split("\n"):
                if line:
                    if not merge_str:
                        pos = line.find(" = ")
                        if line[:pos].lstrip() == "Parameters":
                            merge_key = line[:pos].lstrip()
                            merge_str = line[pos + 3:].lstrip()
                        else:
                            data_dict.update(
                                {line[:pos].lstrip(): line[pos + 3:].lstrip()})
                    else:
                        data_dict.update(
                            {merge_key: merge_str + "\n" + line})
                        merge_key = ""
                        merge_str = ""
                else:
                    data_list.append(data_dict)
                    data_dict = {}
                    merge_key = ""
                    merge_str = ""

            for info in data_list:
                if info["Schedule_ID"] not in schedule_info:
                    if info["Schedule_Type"] == "2":
                        schedule_type = "snapshot"
                    elif info["Schedule_Type"] == "6":
                        schedule_type = "replication"
                    else:
                        schedule_type = "validation"
                    resource = "/protection/nodes/{0}/{1}/".format(
                        info["Primary_Name"], schedule_type)
                    status = "active" if info["State"] == "1" else "suspended"
                    description = "Create a schedule {0}".format(schedule_type)
                    schedule_info.update({info["Schedule_ID"]:
                                          {"id": info["Schedule_ID"],
                                           "resource": resource,
                                           "schedule": info["Expression"],
                                           "started_at":
                                           int(info["Next_Run_Time"]),
                                           "updated_at":
                                           int(info["Last_Run_time"]),
                                           "description": description,
                                           "status": status
                                           }})

        except Exception:
            return False, {}

        return True, schedule_info

    def crontab_schedule_list(self):
        cmd = (("crontab -u %s -l"
                " | grep '#PDA Schedule'")
               % (self.user))

        status, data = self._send_ssh_cmd(cmd)
        if not status:
            _logger.error(
                "Fail to list schedule.")

        return status, data

    def ssh_create_snapshot(self, hostname):
        ret_status = True
        cmd = (('curl -X POST http://127.0.0.1:5678/ '
                '-d \'{"id":11,"jsonrpc":"2.0",'
                '"method":"create_snapshot",'
                '"params":{"cluster_name":"%s"}}\'')
               % (hostname))

        status, data = self._send_ssh_cmd(cmd)
        if not status or "error" in data:
            _logger.error("Fail to create snapshot. hostname: %s" % (hostname))
            ret_status = False

        return ret_status, data

    def ssh_add_schedule(self, hostname, schedule):
        cmd = (('(crontab -u %s -l ; '
                'echo \"%s curl -X POST http://127.0.0.1:5678/ '
                '-d \'{\\"id\\":11,\\"jsonrpc\\":\\"2.0\\",'
                '\\"method\\":\\"create_snapshot\\",'
                '\\"params\\":{\\"cluster_name\\":\\"%s\\"}}\' '
                '#PDA Schedule %s\") | crontab -u %s -')
               % (self.user, schedule, hostname, hostname, self.user))

        status, data = self._send_ssh_cmd(cmd)
        if not status:
            _logger.error(
                "Fail to add schedule. (hostname: %s, schedule: %s)"
                % (hostname, schedule))

        return status, data

    def ssh_delete_schedule(self, hostname):
        cmd = (("crontab -u %s -l"
                " | grep -v '#PDA Schedule %s'  "
                "| crontab -u %s -")
               % (self.user, hostname, self.user))

        status, data = self._send_ssh_cmd(cmd)
        if not status:
            _logger.error(
                "Fail to delete schedule. (hostname: %s)"
                % (hostname))

        return status, data

    def _send_ssh_cmd(self, cmd):
        status = False
        data = ""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if self.auth == "password":
                ssh.connect(self.host, username=self.user,
                            password=self.passwd)
            else:
                private_key = StringIO.StringIO(self.key)
                mykey = paramiko.RSAKey.from_private_key(private_key)
                ssh.connect(self.host, username=self.user,
                            pkey=mykey)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            _logger.debug("Run ssh command %s)" % (cmd))

            if (stdout.channel.recv_exit_status() == 0) or \
                    (stdout.channel.recv_exit_status() == 1 and
                     not stderr.read()):
                status = True
                data = stdout.read()
            else:
                status = False
                data = stderr.read()
            ssh.close()

        except Exception:
            _logger.error(
                "ssh error happened. command: %s" % (cmd))

        return status, data


if __name__ == '__main__':
    try:
        drp_api = DRPAPIClient("api")
        print("========== API Authetication ==========")
        print drp_api.drp_login()

        print("========== API protection list ==========")
        status, protection = drp_api.get_protection_list()
        print status, json.dumps(protection)

        print("========== API protection detail ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, protection = drp_api.get_protection_detail(hostname)
        print status, json.dumps(protection)

        print("========== API add schedule ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        schedule = "* * * * *"
        status, schedules = drp_api.get_add_schedule(
            hostname, schedule)
        print status, json.dumps(schedules)

        print("========== API create snapshot ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, snapshot = drp_api.get_create_snapshot(hostname)
        print status, snapshot

        print("========== API schedules list ==========")
        status, schedules = drp_api.get_schedule_list()
        print status, json.dumps(schedules)

        print("========== Update schedule ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        schedule = "* * 3 4 5"
        status, schedules = drp_api.v1_update_schedule(
            hostname, schedule, "activate")
        print status, schedules

        print("========== API delete schedule ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, schedules = drp_api.get_delete_schedule(hostname)
        print status, schedules
        '''
        drp_ssh = DRPAPIClient("password")
        print("========== SSH Authetication ==========")
        print drp_ssh.drp_login()

        print("========== SSH protection list ==========")
        status, protection = drp_ssh.get_protection_list()
        print status, json.dumps(protection)

        print("========== SSH protection detail ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, protection = drp_ssh.get_protection_detail(hostname)
        print status, json.dumps(protection)

        print("========== SSH add schedule ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        schedule = "* * * * *"
        status, schedules = drp_ssh.get_add_schedule(
            hostname, schedule)
        print status, json.dumps(schedules)

        print("========== SSH create snapshot ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, snapshot = drp_ssh.get_create_snapshot(hostname)
        print status, snapshot

        print("========== SSH schedules list ==========")
        status, schedules = drp_ssh.get_schedule_list()
        print status, json.dumps(schedules)

        print("========== SSH delete schedule ==========")
        hostname = "420df110-3cc1-c97b-6ac7-bd2faf5f4c87"
        status, schedules = drp_ssh.get_delete_schedule(hostname)
        print status, schedules
        '''
    except Exception:
        _logger.exception('drpclient')
        # traceback.print_exc()
        formatted_lines = traceback.format_exc().splitlines()
        print(formatted_lines[-1])
