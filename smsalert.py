# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import base64
from httplib import HTTPSConnection
from httplib import OK, CREATED
from httplib import responses as httplib_responses
import json
import ssl
import traceback
from urllib import urlencode
from urlparse import urlparse

import config
from eventmanager import EventDb
import logger


# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()


def http_status_string(status):
    if not isinstance(status, int):
        return str(status)

    try:
        status_str = httplib_responses[status]
        return status_str
    except KeyError:
        # not defined by httplib
        return str(status)


class MailError(Exception):
    def __init__(self, msg):
        self.msg = msg


class SmsAlert(object):
    """
    Sms Alert V1
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

        self.sms = self.get_sms()
        self.sms_format = self.get_sms_format()
        self.sms_dict = {}

    def get_sms(self):
        endpoint = _conf.get('service.sms.twilio', 'endpoint', None)
        account = _conf.get('service.sms.twilio', 'account', None)
        token = _conf.get('service.sms.twilio', 'token', None)
        caller = _conf.get('service.sms.twilio', 'caller', None)

        if endpoint and account and token and caller:
            return {'endpoint': endpoint,
                    'account': account,
                    'token': token,
                    'caller': caller}
        else:
            return None

    def get_sms_format(self):
        sms_dict = {}
        for i in _conf.sections():
            if i.startswith("action.sms"):
                sms_dict.update({i: {}})
                target = sms_dict[i]
                for (key, val) in _conf.items(i):
                    target.update({key: val})
        return sms_dict

    def get_sms_dict(self, send_list):
        self.sms_dict = {}
        for sms_data in send_list:
            if sms_data[0] not in self.sms_dict:
                self.sms_dict.update(
                    {sms_data[0]: {}})
            target_host = self.sms_dict[sms_data[0]]
            if sms_data[3].keys()[0] not in target_host:
                target_host.update(
                    {
                        sms_data[3].keys()[0]: {
                            "disks": [],
                            "display": sms_data[2]["display"],
                            "policy_name": sms_data[2]["policy_name"],
                            "policy_status": sms_data[2]["policy_status"]}})
            target = target_host[sms_data[3].keys()[0]]
            for disk_list in sms_data[3].values():
                for disk, status in disk_list.iteritems():
                    target["disks"].append({disk: {"near_failure": status["near_failure"],
                                                   "disk_name": status["disk_name"]}})

    def send_message(self, send_list):
        if not self.sms:
            if send_list:
                _logger.warning("[SmsAlert] SMS info is not sufficient.")
            return None
        elif send_list:
            eventdb_conn = EventDb()
            for sms_data in send_list:
                eventdb_conn.push_queue(sms_data[4])
                eventdb_conn.db_streaming()
                sms_data[4]["status"] = sms_data[4]["status_failed"]
        else:
            return None
        try:
            self.get_sms_dict(send_list)
            for sms_data in send_list:
                host_vm_name = "{0} ({1})".format(
                    sms_data[1].keys()[0], sms_data[1].values()[0]["vm_name"]) \
                    if sms_data[1].values()[0]["vm_name"] \
                    else "{0}".format(sms_data[1].keys()[0])
                disk_status = ""
                for disk in self.sms_dict[
                    sms_data[0]][
                        sms_data[3].keys()[0]]["disks"]:
                    for status in disk.values():
                        disk_status += "{0} ({1}), ".format(
                            status["disk_name"], status["near_failure"])

                replaced = {"host": host_vm_name,
                            "disks": disk_status[:-2],
                            "display": self.sms_dict[
                                sms_data[0]][sms_data[3].keys()[0]]["display"] + '\n',
                            "policy_name":
                            self.sms_dict[sms_data[0]][
                                sms_data[3].keys()[0]]["policy_name"],
                            "policy_status":
                            self.sms_dict[sms_data[0]][
                                sms_data[3].keys()[0]]["policy_status"]}

                body = self.sms_format[sms_data[0]]["body"].format(
                    **replaced)

                server = SMSTwilio(self.sms)

                url = self.sms["endpoint"]

                params = {'To': self.sms_format[sms_data[0]]["to"],
                          'From': self.sms["caller"],
                          'Body': body}

                status, data, header = server.send(
                    'POST', url, params, [OK, CREATED])

                if status == OK or status == CREATED:
                    _logger.info(
                        "[SmsAlert] Successfully sent message.")
                    sms_data[4]["status"] = sms_data[4]["status_succeeded"]
                else:
                    _logger.error("[SmsAlert] Failed to send message.")
                    _logger.debug("status %d" % (status))
        except Exception as e:
            _logger.error("[SmsAlert] Failed to send message.")
            _logger.debug("%s" % (e))
        eventdb_conn = EventDb()
        for sms_data in send_list:
            eventdb_conn.push_queue(sms_data[4])
        eventdb_conn.db_streaming()


class SMSTwilio(object):
    """
    Twilio SMS class
    """

    def __init__(self, sms_setting=None):
        if sms_setting:
            self.endpoint = sms_setting["endpoint"]
            self.user = sms_setting["account"]
            self.passwd = sms_setting["token"]
            self.caller = sms_setting["caller"]
        else:
            self._get_endpoint()

    def send(self, method, url, params, expected_status):
        """
        Send API command
        """
        retcode, data, ret_header = OK, {}, {}
        header, body = {}, None

        # preparing request

        _url = urlparse(url)
        path = _url.path + ("?%s" % _url.query if _url.query else "")

        # base64 encode the username and password
        auth = base64.encodestring('%s:%s'
                                   % (self.user,
                                      self.passwd)).replace('\n', '')

        # header = {"User-Agent": "python-requests/2.10.0",
        #          "Content-type": "application/x-www-form-urlencoded; charset=UTF-8"}
        header = {"User-Agent": "python-requests/2.10.0",
                  "Content-type": "application/x-www-form-urlencoded; charset=UTF-8"}
        header['Authorization'] = 'Basic %s' % auth

        body = urlencode(params).encode()

        _logger.debug("twilio-sms: URL(%s) method(%s) header(%s) body(%s)" % (
            path, method, header, body))

        # send request
        conn = HTTPSConnection(_url.hostname, _url.port, timeout=120,
                               context=ssl._create_unverified_context())
        conn.request(method, path, body, header)

        response = conn.getresponse()
        data = response.read()
        retcode = response.status
        hdr = response.getheaders()
        #reason = response.reason

        _logger.debug("twilio-sms: response(%s) header(%s) output(%s)" %
                      (http_status_string(retcode), str(hdr), str(data)))

        # close connection
        if conn:
            conn.close()

        # format data
        if data:
            try:
                data = json.loads(data)
            except ValueError:
                # not JSON format
                pass
        else:
            data = {}

        if hdr:
            ret_header = {hdr[i][0]: hdr[i][1] for i in range(len(hdr))}

        if retcode not in expected_status:
            _logger.warning(
                "twilio-sms: %s %s returns response %s (expects: %s)" %
                (method, url, str(retcode), str(expected_status)))

        return retcode, data, ret_header

    def _get_endpoint(self):
        self.endpoint = "%s" % _conf.get("service.sms.twilio", "endpoint")
        self.user = "%s" % _conf.get("service.sms.twilio", "account")
        self.passwd = "%s" % _conf.get("service.sms.twilio", "token")
        self.caller = "%s" % _conf.get("service.sms.twilio", "caller")


if __name__ == '__main__':

    try:
        sms = SMSTwilio()

        params = {
            'To': '+886939964688',
            'From': sms.caller,
            'Body': 'This is a test message.'
        }

        #url = "%s/?%s" % (sms.endpoint, urlencode(params))
        url = sms.endpoint
        status, data, header = sms.send('POST', url, params, [OK, CREATED])

    except Exception:
        _logger.exception('twilio-sms')
        # traceback.print_exc()
        formatted_lines = traceback.format_exc().splitlines()
        print(formatted_lines[-1])
