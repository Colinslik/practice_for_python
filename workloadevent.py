# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import json
import time

import dpclient
try:
    import logger
    ifttt = False
except Exception:
    ifttt = True

if not ifttt:
    from eventmanager import Event as Event_obj
    from eventmanager import Event_Manager

    # Get configuration and logger
    _logger = logger.get_logger()
else:
    from operator import itemgetter

    class Event_Manager(object):
        def __init__(self):
            pass


class WorkloadEvent(Event_Manager):

    def __init__(self, host_list, host="127.0.0.1"):
        super(WorkloadEvent, self).__init__()
        self.host = host
        if "*" in host_list:
            self._host_list = []
        else:
            self._host_list = host_list
        self.event_filter = None

    def query_relationship(self):
        domainid_hosts = {}

        start_time = int(time.time() * 1000000000) - (3600 * 24 * 1000000000)
        cond_dict = {"time": start_time}

        relation_conn = Neo4jTool(self.host)
        host_dict = relation_conn.query_nodes(
            "VMHost", "name", self._host_list, cond_dict)

        for data in host_dict:
            domainid_hosts.update({data["domainId"]: data["name"]})

        return domainid_hosts

    def setup_event_filter(self, filter_dict):
        self.event_filter = {}
        if "type" in filter_dict and filter_dict["type"] != "*":
            self.event_filter.update({"event_type": filter_dict["type"]})
        if "level" in filter_dict and filter_dict["level"] != "*":
            self.event_filter.update({"event_level": filter_dict["level"]})

    def query_eventdb(self):
        eventdb_conn = InfluxdbTool()

        end_time = int(time.time() * 1000000000)
        start_time = end_time - (3600 * 1000000000)
        result = eventdb_conn.query_event_by_interval(
            [start_time, end_time], self.event_filter)

        if result:
            for val in result.values():
                val["details"] = ' '.join(val["details"])
                val["event_type"] = ' '.join(val["event_type"])
        return result

    if not ifttt:
        def event_trigger(self):
            pass
    else:
        def event_trigger(self):
            event_list = []
            event_result = self.query_eventdb()
            if event_result:
                for time, data in event_result.iteritems():
                    event_list.append({
                        "time": time,
                        "event_type": data["event_type"],
                        "event_level": data["event_level"][0],
                        "title": data["details"],
                        "agenthost": data["agenthost"][0]
                    })
            newlist = sorted(event_list, key=itemgetter('time'), reverse=True)
            return newlist


class Neo4jTool(dpclient.Neo4jApi):
    """
    DiskProphet connector V1 API
    """

    if not ifttt:
        def __init__(self, host="127.0.0.1"):
            super(Neo4jTool, self).__init__(host)
    else:
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
            if not ifttt:
                _logger.error("Get no results from DiskProphet.")
            else:
                print "Get no results from DiskProphet."
            raise dpclient.DbError(msg)
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
            if not ifttt:
                _logger.error("Get no results from DiskProphet.")
            else:
                print "Get no results from DiskProphet."
            raise dpclient.DbError(msg)
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
            if not ifttt:
                _logger.error("Get no results from DiskProphet.")
            else:
                print "Get no results from DiskProphet."
            raise dpclient.DbError(msg)
        else:
            encoded_list = []
            for i in encoded["results"][0]["data"]:
                encoded_list.append(str(i["graph"]["nodes"][0]["id"]))

            return encoded_list


class InfluxdbTool(dpclient.InfluxdbApi):
    """
    DiskProphet connector V1 API
    """

    if not ifttt:
        def __init__(self, host="127.0.0.1"):
            super(InfluxdbTool, self).__init__(host)
    else:
        def __init__(self, host="127.0.0.1", port="8086", user="dpInfluxdb", password="DpPassw0rd"):
            super(InfluxdbTool, self).__init__(
                host, port, user, password, True)

    def query_event_by_interval(self, time_interval, filter_dict):
        event_dict = {}
        try:
            result = self.query_data_detail_setup("sai_event",
                                                  time_interval,
                                                  filter_dict, None, None,
                                                  None, True, True)
        except dpclient.DbError:
            return None

        else:
            key_list = []
            for key in result.keys():
                if "time" != key:
                    key_list.append(key)
            for i in range(len(result["time"])):
                event_dict.update({result["time"][i][0]: {}})
                target = event_dict[result["time"][i][0]]
                for j in key_list:
                    target.update({j: result[j][i]})
            return event_dict


if __name__ == '__main__':
    if ifttt:
        try:
            workloadevent_conn = WorkloadEvent("*")
            workload_result = workloadevent_conn.event_trigger()

            if workload_result:
                print workload_result
        except dpclient.DbError as e:
            print e.msg
