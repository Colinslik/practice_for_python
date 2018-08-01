# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from copy import deepcopy
import json

import requests
import urllib3

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

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        if not self.host or not self.port \
                or not self.user or not self.password:
            msg = "DiskProphet read config error. " \
                "dp.neo4j.host: {0}  dp.neo4j.port: {1}  "\
                "dp.neo4j.user: {2}".format(
                    self.host, self.port, self.user)
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

        try:
            result = requests.post(
                url, data=json.dumps(_params), headers=headers)
        except requests.RequestException:
            pass
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
                return None


class Neo4jTool(Neo4jApi):
    """
    DiskProphet connector V1 API
    """

    def __init__(self, host="127.0.0.1", port=7474, user="neo4j", password="na"):
        super(Neo4jTool, self).__init__(host, port, user, password)

    def query_nodes(self, label="VMVirtualMachine", key="domainId", val=[],
                    cond_dict={}):
        result = self._send_cmd(self._detail_setup(
            label, None, key, val, cond_dict,
            None, None, None, None))

        try:
            encoded = json.loads(result.text, encoding='utf-8')
            if "id" in encoded["results"][0]["data"][0]["graph"]["nodes"][0]:
                pass
        except Exception:
            msg = "Neo4jApi Get no results ( {0}.{1} )".format(
                self.__class__.__name__,
                self.query_nodes.__name__)
            raise DbError(msg)
        else:
            encoded_list = []
            for i in encoded["results"][0]["data"]:
                encoded_list.append(i["graph"]["nodes"][0]["properties"])

        return encoded_list

    def query_by_label_rels(self, vms=[], label="VMVirtualMachine",
                            rels=None, cond_dict={},
                            tag="domainId", output_tag="domainId"):
        if rels:
            left_rel = ""
            right_rel = ""
            result_type = " n"
            for idx, rel in enumerate(rels):
                left_rel += "-[r{1}:{0}]->(t{1})".format(rel, idx)
                right_rel += "<-[r{1}:{0}]-(t{1})".format(rel, idx)
                result_type += ",r{0},t{0}".format(idx)
            _rels = [left_rel, right_rel]
        else:
            _rels = ["-[r*]->(t)", "<-[r*]-(t)"]
            result_type = " n,r,t"
        params = ""
        arranged = {}

        for i in _rels:
            params = self._detail_setup(
                label, i, None, None, None,
                self._query_id_by_key(label, tag, vms, cond_dict),
                result_type, None, params)

        result = self._send_cmd(params)

        try:
            encoded = json.loads(result.text, encoding='utf-8')
            if tag in encoded["results"][0][
                    "data"][0]["graph"]["nodes"][0]["properties"]:
                pass
        except Exception:
            msg = "Neo4jApi Get no results ( {0}.{1} )".format(
                self.__class__.__name__,
                self.query_by_label_rels.__name__)
            raise DbError(msg)
        else:
            labels_pool = {}
            target = {}
            key = ""
            count = 0
            for i in encoded["results"][0]["data"]:
                for j in i["graph"]["nodes"]:
                    labels_pool[str(j["labels"][0])] = str(
                        j["properties"][output_tag])

                key = labels_pool.get(label)
                if key in arranged:
                    pass
                else:
                    arranged[key] = {}
                target = arranged.get(key)
                count += 1

                if "VMDatastore" in labels_pool:
                    key = labels_pool.get("VMDatastore")
                    if "Datastores" not in target:
                        target["Datastores"] = {}
                    if key in target["Datastores"]:
                        pass
                    else:
                        target["Datastores"][key] = {"Disks": {}}

                if "VMDisk" in labels_pool:
                    if "VMDatastore" in labels_pool:
                        key = labels_pool.get("VMDatastore")
                    else:
                        key = "None_#{0}".format(count)
                    if "Datastores" not in target:
                        target["Datastores"] = {}
                    if key in target["Datastores"]:
                        pass
                    else:
                        target["Datastores"][key] = {"Disks": {}}
                    target["Datastores"][key]["Disks"][
                        labels_pool.get("VMDisk")] = {"Groups": ""}

                if "VMVSanDiskGroup" in labels_pool:
                    if "VMDisk" in labels_pool:
                        key_disks = labels_pool.get("VMDisk")
                    else:
                        key_disks = "None_#{0}".format(count)
                    if "VMDatastore" in labels_pool:
                        key = labels_pool.get("VMDatastore")
                    else:
                        key = "None_#{0}".format(count)
                    if "Datastores" not in target:
                        target["Datastores"] = {}
                    if key not in target["Datastores"]:
                        target["Datastores"][key] = {"Disks": {}}
                    if key_disks not in target["Datastores"][key]["Disks"]:
                        target["Datastores"][key]["Disks"][key_disks] = {
                            "Groups": ""}
                    target["Datastores"][key]["Disks"][key_disks]["Groups"] = \
                        labels_pool.get("VMVSanDiskGroup")

                if "VMVirtualMachine" in labels_pool:
                    key = labels_pool.get("VMVirtualMachine")
                    if "VMs" not in target:
                        target["VMs"] = []
                    if key not in target["VMs"]:
                        target["VMs"].append(key)

                if "VMClusterCenter" in labels_pool:
                    if "Clusters" not in target:
                        target["Clusters"] = {}
                    target["Clusters"][labels_pool.get(
                        "VMClusterCenter")] = {"Types": "Cluster"}

                if "VMHost" in labels_pool:
                    if "Clusters" not in target:
                        target["Clusters"] = {}
                    target["Clusters"][labels_pool.get(
                        "VMHost")] = {"Types": "Host"}

                labels_pool.clear()

            return arranged

    def _query_id_by_key(self, label, key, val,
                         cond_dict={}):
        params = self._detail_setup(
            label, None, key, val, cond_dict,
            None, None, None, None)
        result = self._send_cmd(params)

        try:
            encoded = json.loads(result.text, encoding='utf-8')
            if "id" in encoded["results"][0]["data"][0]["graph"]["nodes"][0]:
                pass
        except Exception:
            msg = "Neo4jApi Get no results ( {0}.{1} )".format(
                self.__class__.__name__,
                self._query_id_by_key.__name__)
            raise DbError(msg)
        else:
            encoded_list = []
            for i in encoded["results"][0]["data"]:
                encoded_list.append(str(i["graph"]["nodes"][0]["id"]))

            return encoded_list


class InfluxdbApi(object):
    """
    DiskProphet connector V1 API
    """

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
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
                            pass
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
            pass
        else:
            if result.status_code is 200:
                return result
            else:
                return None

    def _post_cmd(self, params):
        url = '{0}://{1}:{2}/write?db={3}&u={4}&p={5}&precision=ns'.format(
            self.ssl, self.host, self.port, self.dbname,
            self.user, self.password)

        _params = '{0}'.format(params)

        try:
            result = requests.request(
                "POST", url, data=_params, verify=False)

        except requests.RequestException as e:
            pass
        else:
            if result.status_code is 204:
                print "Write Completed."
            else:
                print "Write Failed. The status code is ", result.status_code


class InfluxdbTool(InfluxdbApi):
    """
    DiskProphet connector V1 API
    """

    def __init__(self, host="127.0.0.1", port="8086", user="dpInfluxdb", password="DpPassw0rd"):
        super(InfluxdbTool, self).__init__(host, port, user, password)

    def query_vm_host(self, composite_cmd="virtualmachine",
                      entry_list=None, group=None):
        if entry_list:
            entry_list_pkg = ["domain_id", entry_list]
        else:
            entry_list_pkg = entry_list
        return self.query_data_detail_setup(composite_cmd,
                                            None,
                                            entry_list_pkg, group, None,
                                            1, True, True)

    def query_disk_info(self, composite_cmd="sai_disk",
                        entry_list=None, group=None):
        if entry_list:
            entry_list_pkg = ["disk_domain_id", entry_list]
        else:
            entry_list_pkg = entry_list
        return self._postprocess_to_filter_id(
            self.query_data_detail_setup(composite_cmd,
                                         None,
                                         entry_list_pkg, group, None,
                                         1, True, True))

    def query_ip_by_host(self, time_interval, entry_list=None):
        ip_dict = {}
        filter_list = ["domain_id", entry_list]
        try:
            result = self.query_data_detail_setup("sai_host",
                                                  time_interval,
                                                  filter_list, None, None,
                                                  None, True, True)
        except DbError:
            msg = "InfluxdbApi Get no results ( {0}.{1} )" \
                " Your URL is {2}".format(
                    self.__class__.__name__,
                    self.query_data_detail_setup.__name__, self.url)
            raise DbError(msg)

        else:
            key_list = []
            for key in result.keys():
                if "domain_id" != key:
                    key_list.append(key)
            for i in range(len(result["domain_id"])):
                if result["domain_id"][i][0] not in ip_dict:
                    ip_dict.update({result["domain_id"][i][0]: {}})
                target = ip_dict[result["domain_id"][i][0]]
                for j in key_list:
                    target.update({j: result[j][i]})
            return ip_dict

    def query_pred_interval_by_disk(self, time_interval, entry_list=None):
        if entry_list:
            entry_list_pkg = ["disk_domain_id", entry_list]
        else:
            entry_list_pkg = entry_list
        return self._postprocess_to_filter_id(
            self.query_data_detail_setup('sai_disk_prediction',
                                         time_interval,
                                         entry_list_pkg, None, None,
                                         None, True, True))

    def _postprocess_to_filter_id(self, pre_result):
        pos_result = {}

        # except for instruction error
        if pre_result is None:
            msg = "InfluxdbApi Get no results ( {0}.{1} )" \
                " Your URL is {2}".format(
                    self.__class__.__name__,
                    self.query_data_detail_setup.__name__, self.url)
            raise DbError(msg)

        for idx, val in enumerate(pre_result.get('disk_domain_id')):
            if val[0] not in pos_result:
                pos_result[val[0]] = {}

            if pre_result.get(
                    'host_domain_id')[idx][0] not in pos_result[val[0]]:
                pos_result[val[0]][
                    pre_result.get('host_domain_id')[idx][0]] = {}

            target_host = pos_result[val[0]][
                pre_result.get('host_domain_id')[idx][0]]

            if pre_result.get('time')[idx][0] not in target_host:
                target_host[pre_result.get('time')[idx][0]] = {}

            target = target_host[pre_result.get('time')[idx][0]]

            keyslist = [x for x in pre_result.keys() if x not in [
                'disk_domain_id', 'host_domain_id', 'time']]

            for key in keyslist:
                target.update({key: pre_result.get(key)[idx]})

        return pos_result


if __name__ == '__main__':
    pass
