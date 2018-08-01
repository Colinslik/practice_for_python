# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import json

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DpApi(object):
    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password

    def login(self):
        url = '{0}/login'.format(self.url)

        headers = {"Accept": "application/json, text/plain, */*",
                   "Content-Type": "application/json;charset=UTF-8"
                   }

        params = {'username': self.username,
                  'password': self.password}

        try:
            result = requests.post(
                url, data=json.dumps(params), headers=headers, verify=False)
        except Exception:
            return None
        else:
            if result.status_code is 200:
                return result.cookies
            else:
                return None

    def send_cmd(self, url_arg, token):
        url = '{0}/{1}'.format(self.url, url_arg)

        headers = {"Accept": "application/json, text/plain, */*",
                   "Content-Type": "application/json;charset=UTF-8"
                   }

        cookies = token

        try:
            result = requests.get(
                url, headers=headers, cookies=cookies, verify=False)
        except Exception:
            return None
        else:
            if result.status_code is 200:
                return result.text
            else:
                return None


if __name__ == '__main__':
    pass
