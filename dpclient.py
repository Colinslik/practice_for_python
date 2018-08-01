# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from copy import deepcopy
import json

import requests
import urllib3

import config
import logger


# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DbError(Exception):
    def __init__(self, msg):
        self.msg = msg


class ConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg


class Neo4jApi(object):
    """
    DiskProphet connector V1 API
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

        self.host = _conf.get('service.diskprophet', 'dp.neo4j.host', '')
        if not self.host:
            self.host = _conf.get('service.diskprophet', 'dp.host', '')
        self.port = _conf.get('service.diskprophet', 'dp.neo4j.port', '')
        self.user = _conf.get('service.diskprophet', 'dp.neo4j.user', '')
        self.password = _conf.get('service.diskprophet', 'dp.neo4j.passwd', '')
        if not self.host or not self.port \
                or not self.user or not self.password:
            msg = "DiskProphet read config error. " \
                "dp.neo4j.host: {0}  dp.neo4j.port: {1}  "\
                "dp.neo4j.user: {2}".format(
                    self.host, self.port, self.user)
            _logger.error("Invalid access to DiskProphet.")
            raise ConfigError(msg)

    def _detail_setup(self, label=None, _rels=None,
                      key=None, val=None, cond_dict=None, idx=None,
                      result_type=None, limit_number=None,
                      union_args=None):

        if label:
            self.label = ":{0}".format(label)
        else:
            self.label = ""

        if _rels:
            self._rels = " {0}".format(_rels)
        else:
            self._rels = ""

        if key and val:
            self.key = ' WHERE '
            for i in val:
                if isinstance(i, int) or isinstance(val, long):
                    self.key += "(n.{0} = {1}".format(key, i)
                else:
                    self.key += "(n.{0} = '{1}'".format(key, i)
                if cond_dict:
                    for dict_key, dict_val in cond_dict.iteritems():
                        if "time" in dict_key:
                            operator = ">="
                        else:
                            operator = "="
                        if isinstance(dict_val, int) or \
                                isinstance(dict_val, long):
                            self.key += " AND n.{0} {1} {2}".format(
                                dict_key, operator, dict_val)
                        else:
                            self.key += " AND n.{0} {1} '{2}'".format(
                                dict_key, operator, dict_val)
                self.key += ") OR "
            self.key = self.key[:-4]
        else:
            if cond_dict:
                for dict_key, dict_val in cond_dict.iteritems():
                    if "time" in dict_key:
                        operator = ">="
                    else:
                        operator = "="
                    if isinstance(dict_val, int) or \
                            isinstance(dict_val, long):
                        self.key = ' WHERE (n.{0} {1} {2})'.format(
                            dict_key, operator, dict_val)
                    else:
                        self.key = " WHERE (n.{0} {1} '{2}')".format(
                            dict_key, operator, dict_val)
            else:
                self.key = ""

        if idx:
            if self.key:
                self.idx = " AND WHERE ID(n) = {0}".format(idx[0])
            else:
                self.idx = " WHERE ID(n) = {0}".format(idx[0])
            for i in idx[1:]:
                self.idx += " OR ID(n) = {0}".format(i)
        else:
            self.idx = ""

        if result_type:
            self.result_type = result_type
        else:
            self.result_type = " n"

        if limit_number:
            self.limit_number = " LIMIT {0}".format(limit_number)
        else:
            self.limit_number = ""

        if union_args:
            self.union_args = " UNION {0}".format(union_args)
        else:
            self.union_args = ""

        params = "MATCH (n{0}){1}{2}{3} RETURN{4}{5}{6}".format(
            self.label, self._rels, self.key, self.idx,
            self.result_type, self.limit_number, self.union_args)

        return params

    def _send_cmd(self, params):

        url = 'http://{0}:{1}@{2}:{3}/db/data/transaction/commit'.format(
            self.user, self.password, self.host, self.port)

        headers = {"Accept": "application/json; charset=UTF-8",
                   "Content-Type": "application/json"
                   }

        _params = {
            "statements": [{
                "statement": "{0}".format(params),
                "resultDataContents": ["graph"]
            }]
        }

        _logger.debug("Neo4jApi: URL (%s)"
                      " Params (%s)" % (url, _params))

        try:
            result = requests.post(
                url, data=json.dumps(_params), headers=headers)
        except requests.RequestException:
            _logger.error("DB Request error.")
            _logger.debug("Error Code: %s" % (result.status_code))
        except Exception:
            msg = "Neo4jApi Get no results ( {0}.{1} )" \
                " Your Params is {2}".format(
                    self.__class__.__name__,
                    self._send_cmd.__name__, _params)
            raise DbError(msg)
        else:
            if result.status_code is 200:
                return result
            else:
                _logger.error("DB Request error.")
                _logger.debug("Error Code: %s" % (result.status_code))
                return None


class InfluxdbApi(object):
    """
    DiskProphet connector V1 API
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

        self.host = _conf.get('service.diskprophet', 'dp.influxdb.host', '')
        if not self.host:
            self.host = _conf.get('service.diskprophet', 'dp.host', '')
        self.port = _conf.get('service.diskprophet', 'dp.influxdb.port', '')
        self.user = _conf.get('service.diskprophet', 'dp.influxdb.user', '')
        self.password = _conf.get(
            'service.diskprophet', 'dp.influxdb.passwd', '')
        self.dbname = "telegraf"
        if self.password:
            self.ssl = "https"
        else:
            self.ssl = "http"

        self.set_display(None)

        if not self.host or not self.port \
                or not self.user or not self.password:
            msg = "DiskProphet read config error. " \
                "dp.influxdb.host: {0}  dp.influxdb.port: {1}  "\
                "dp.influxdb.user: {2}".format(
                    self.host, self.port, self.user)
            _logger.error("Invalid access to DiskProphet.")
            raise ConfigError(msg)

    def set_display(self, display_list):
        self.display_list = ""

        if display_list and type(display_list) is list:
            if "*" == display_list[0]:
                self.display_list += '{0}'.format(display_list[0])
            else:
                self.display_list += '"{0}"'.format(display_list[0])
            for i in display_list[1:]:
                self.display_list += ', "{0}"'.format(i)
        elif display_list and type(display_list) is str:
            self.display_list = display_list
        else:
            self.display_list = "*"

    def setup_argument(self, tablename, time_interval,
                       entry_list_pkg=None, group=None,
                       slimit_number=None, limit_number=None,
                       sort=False):
        # setup influxdb cli arguments

        if entry_list_pkg and type(entry_list_pkg) is list:
            self.entry_list = \
                ' WHERE ("{0}"::tag = \'{1}\' or "{0}"::field = \'{1}\')'.format(
                    entry_list_pkg[0], entry_list_pkg[1][0])
            for i in entry_list_pkg[1][1:]:
                self.entry_list += \
                    ' OR ("{0}"::tag = \'{1}\' or "{0}"::field = \'{1}\')'.format(
                        entry_list_pkg[0], i)
        elif entry_list_pkg and type(entry_list_pkg) is dict:
            self.entry_list = ' WHERE '
            for key, val in entry_list_pkg.iteritems():
                self.entry_list += \
                    '("{0}"::tag = \'{1}\' or "{0}"::field = \'{1}\') AND '.format(
                        key, val)
            self.entry_list = self.entry_list[:-5]
        else:
            self.entry_list = ""

        if time_interval and entry_list_pkg:
            self.time_interval = " AND time >= {0} AND time <= {1}".format(
                time_interval[0], time_interval[1])
        elif time_interval and entry_list_pkg is None:
            self.time_interval = " WHERE time >= {0} AND time <= {1}".format(
                time_interval[0], time_interval[1])
        else:
            self.time_interval = ""

        if group:
            self.group = " GROUP BY {0}".format(group[0])
            for i in group[1:]:
                self.group += ", {0}".format(i)
        else:
            self.group = ""

        if group and slimit_number:
            self.slimit_number = " SLIMIT {0}".format(slimit_number)
        else:
            self.slimit_number = ""

        if limit_number:
            self.limit_number = " LIMIT {0}".format(limit_number)
        else:
            self.limit_number = ""

        if sort:
            self.sort = " ORDER BY time DESC"
        else:
            self.sort = ""

        command = 'SELECT {0} FROM {1}{2}{3}{4}{5}{6}{7}'.format(
            self.display_list, tablename, self.entry_list,
            self.time_interval, self.group, self.sort,
            self.limit_number, self.slimit_number)

        return command

    def query_data_detail_setup(self, tablename, time_interval,
                                entry_list_pkg=None, group=None,
                                slimit_number=None, limit_number=None,
                                sort=False, timestamp=True):
        # setup influxdb cli arguments
        if timestamp:
            self.timestamp = "epoch='n'&"
        else:
            self.timestamp = ""

        command = self.setup_argument(tablename, time_interval,
                                      entry_list_pkg, group,
                                      slimit_number, limit_number,
                                      sort)

        self.url = '{0}://{1}:{2}/query?db={3}&u={4}&p={5}&{6}q=' \
            '{7}'.format(
                self.ssl, self.host, self.port, self.dbname, self.user,
                self.password, self.timestamp, command)

        _logger.debug("InfluxdbApi: URL (%s) Argurment:"
                      " Timestamp (%s) Group by (%s)"
                      " Sort (%s) limit (%s) Slimit (%s)"
                      % (self.url, timestamp, group, sort,
                         limit_number, slimit_number))

        result = self._arrange_to_dict()

        # except for instruction error
        if result is None:
            msg = "InfluxdbApi Get no results ( {0}.{1} )" \
                " Your URL is {2}".format(
                    self.__class__.__name__,
                    self.query_data_detail_setup.__name__, self.url)
            raise DbError(msg)
        else:
            return result

    def _arrange_to_dict(self):
        pre_result = {}
        temp = []
        result = self._get_cmd()

        if result:
            try:
                decoded = json.loads(result.text, encoding='utf-8')
                if "results" in decoded and "series" in decoded["results"][0]:
                    decodedjson = decoded["results"][0]["series"]
                    for i in range(len(decodedjson[0]["columns"])):
                        try:
                            for j in range(len(decodedjson)):
                                for k in range(len(decodedjson[j]["values"])):
                                    temp.append(str(decodedjson[j]["values"][k][i])
                                                .replace("u'", "")
                                                .replace("',", "")
                                                .replace("[", "")
                                                .replace("]", "").split())
                        # original entry is a string included Apostrophe, Comma,
                        # Semicolon and character encodeing, replacing them with
                        # split and replace method.
                                pre_result[str(decodedjson[j]["columns"][i])
                                           .replace("u'", "")
                                           .replace("',", "")
                                           .replace("[", "")
                                           .replace("]", "")] = deepcopy(temp)
                        except Exception:
                            _logger.debug(
                                "Unsupported characters are included.")
                            _logger.debug("%s" %
                                          (decodedjson[j]["values"][k][i]))
                        del temp[:]

                    while True:
                        temp_result = pre_result
                        for key in pre_result.keys():
                            dumped_key = "{0}_1".format(key)
                            if dumped_key in pre_result.keys():
                                merged_list = []
                                for idx, val in enumerate(pre_result[key]):
                                    if ["None"] == val:
                                        merged_list.append(
                                            pre_result[dumped_key][idx])
                                    else:
                                        merged_list.append(val)
                                temp_result[key] = merged_list
                                temp_result.pop(dumped_key)
                        if len(temp_result) != len(pre_result):
                            pre_result = temp_result
                        else:
                            break

                    return pre_result
                else:
                    return None
            except Exception:
                msg = "InfluxdbApi Get no results ( {0}.{1} )" \
                    " Your URL is {2}".format(
                        self.__class__.__name__,
                        self.query_data_detail_setup.__name__, self.url)
                raise DbError(msg)
        else:
            return None

    def _get_cmd(self):
        try:
            result = requests.get(self.url, verify=False)
        except requests.RequestException as e:
            _logger.error("DB Request error.")
            _logger.debug("Error Code: %s Result: %s" %
                          (result.status_code, e))
        else:
            if result.status_code is 200:
                return result
            else:
                _logger.error("DB Request error.")
                _logger.debug("Error Code: %s" % (result.status_code))

                return None

    def _post_cmd(self, params):
        url = '{0}://{1}:{2}/write?db={3}&u={4}&p={5}&precision=ns'.format(
            self.ssl, self.host, self.port, self.dbname,
            self.user, self.password)

        _params = '{0}'.format(params)

        _logger.debug("InfluxdbApi POST: URL (%s) Argurment:"
                      " Body (%s)"
                      % (url, _params))
        try:
            result = requests.request(
                "POST", url, data=_params, verify=False)

        except requests.RequestException as e:
            _logger.error("DB Request error.")
            _logger.debug("Result: %s" % (e))
        else:
            if result.status_code is 204:
                print "Write Completed."
                _logger.debug("Write Completed.")
            else:
                print "Write Failed. The status code is ", result.status_code
                _logger.debug("Write Failed. The status code is %d" %
                              (result.status_code))


if __name__ == '__main__':
    pass
