# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
import base64
import codecs
from httplib import HTTPConnection
from httplib import OK, CREATED, NOT_FOUND
from httplib import responses as httplib_responses
import json
import platform
from urlparse import urlparse

from config import Config
from config import UnicodeConfigParser
from define import HOME_DEFAULT, APPLICATION
from define import get_appconf
import logger


# Get logger
_logger = logger.get_logger()


def http_status_string(status):
    if not isinstance(status, int):
        return str(status)

    try:
        status_str = httplib_responses[status]
        return status_str
    except KeyError:
        # not defined by httplib
        return str(status)


class PdaApi(object):
    """
    PDA API client
    """

    def __init__(self):
        self.cookie = ""
        self._get_endpoint()

    def _send_cmd(self, method, url, params, expected_status, json_content=False):
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

        if json_content:
            header = {'Content-Type': 'application/json'}
            header['Authorization'] = 'Basic %s' % auth
            if params:
                body = json.dumps(params, ensure_ascii=False)
                body.encode('utf-8')

        else:
            header = {"User-Agent": "python-requests/2.10.0",
                      "Cookie": self.cookie,
                      "Content-type": "application/x-www-form-urlencoded; charset=UTF-8"}
            header['Authorization'] = 'Basic %s' % auth

            if method.upper() == 'GET':
                if _url.query:
                    path += "&%s" % params if params else ""
                else:
                    path += "?%s" % params if params else ""

            elif method.upper() in ['PUT', 'POST']:
                body = params

        _logger.debug("pdaclient: URL(%s) method(%s) header(%s) body(%s)" % (
            path, method, header, body))

        # send request
        conn = HTTPConnection(_url.hostname, _url.port, timeout=120)
        conn.request(method, path, body, header)

        response = conn.getresponse()
        data = response.read()
        retcode = response.status
        hdr = response.getheaders()
        if not self.cookie:
            self.cookie = response.getheader('set-cookie')
        #reason = response.reason

        #_logger.debug("pdaclient: response(%s) header(%s) output(%s)" %
        #              (http_status_string(retcode), str(hdr), str(data)))

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
                "pdaclient: %s %s returns response %s (expects: %s)" %
                (method, url, str(retcode), str(expected_status)))

        return retcode, data, ret_header

    def _get_endpoint(self):
        conf_files = ["%s/etc/pda.conf.local" % HOME_DEFAULT]
        conf = Config(conf_files)

        self.endpoint = "%s" % conf.get("pda.api", "api.endpoint")
        self.user = "%s" % conf.get("pda.api", "api.user")
        self.passwd = "%s" % conf.get("pda.api", "api.passwd")

    def login(self):
        data = None

        url = "%s/users/%s" % (self.endpoint, self.user)
        params = {}
        status, data, _ = self._send_cmd(
            'GET', url, params, [OK], json_content=True)

        if status not in [OK]:
            _logger.error("status: %s, results: %s" % (status, data))
            raise Exception(data)

        return status, data

    def register_instance(self):
        data = None
        app = get_appconf()

        url = "%s/instances/" % (self.endpoint)
        params = {
            "id": platform.node(),
            "application": app,
            "status": "Online"
        }
        status, data, _ = self._send_cmd(
            'POST', url, params, [CREATED], json_content=True)

        if status not in [CREATED]:
            _logger.error("status: %s, results: %s" % (status, data))
            raise Exception(data)

        return status, data

    def update_instance(self):
        data = None
        app = get_appconf()

        url = "%s/instances/%s" % (self.endpoint, platform.node())
        params = {
            "application": app,
            "status": "Online"
        }
        status, data, _ = self._send_cmd(
            'PUT', url, params, [OK], json_content=True)

        return status, data

    def download_config(self):
        data = None

        url = "%s/%s/configs/pda.conf.user" % (self.endpoint, platform.node())
        params = {}
        status, data, _ = self._send_cmd(
            'GET', url, params, [OK], json_content=True)

        if status == NOT_FOUND:
            _logger.warning("PDA service '%s' is not configured." %
                            platform.node())

        elif status not in [OK]:
            _logger.error("status: %s, results: %s" % (status, data))
            raise Exception(data)

        else:
            # write to local
            user_conf = "%s/etc/pda.conf.user" % HOME_DEFAULT
            config = UnicodeConfigParser()

            for section in data['config']:
                config.add_section(section)
                for key in data['config'][section]:
                    config.set(section, key, "%s" %
                               data['config'][section][key])

            with codecs.open(user_conf, 'w', 'utf-8') as f:
                config.write(f)

            _logger.info("PDA service configuration is downloaded.")

        return status, data

    def update_backups(self, jobinfo):
        for host, info in jobinfo.iteritems():
            joblist = []
            url = "%s/%s/backups/%s" % (self.endpoint, platform.node(), host)
            for data in info:
                '''
                try:
                    datetime_object = datetime.strptime(
                        data["EXEC-TIME"], '%m/%d/%Y %H:%M:%S')
                    execTime = int(time.mktime(datetime_object.timetuple()))
                except Exception:
                    current_time = int(time.time())
                    execTime = current_time
                '''
                joblist.append({
                    "name": data["DESCRIPTION"],
                    "server": data["EXECUTIONHOST"],
                    "id": "" if data["JOBID"] == "0" else data["JOBID"],
                    "no": int(data["JOB#"]),
                    "status": data["STATUS"],
                    #"execTime": execTime,
                    "execTime": data["EXEC-TIME"],
                    "type": data["JOB-TYPE"],
                    "lastResult": data["LAST-RESULT"],
                    "owner": data["OWNER"],
                    "description": data["INFO"] if "INFO" in data else ""
                })

            params = {"backups": joblist}

            _logger.debug("[Pdaclient] url: %s, body: %s" % (url, params))
            status, data, _ = self._send_cmd(
                'PUT', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.error("status: %s, results: %s" % (status, data))


if __name__ == '__main__':

    pda = PdaApi()
    pda.download_config()
