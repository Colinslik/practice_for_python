# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import json
import time

import dpclient
from eventmanager import Event as Event_obj
from eventmanager import Event_Manager
import logger


# Get configuration and logger
_logger = logger.get_logger()


class DiskEvent(Event_Manager):

    def __init__(self, host_list):
        super(DiskEvent, self).__init__()
        if "*" in host_list:
            self._host_list = []
        else:
            self._host_list = host_list
        self._host_info = {}
        self._host_status = {}
        self._prediction_result = {}
        self._ip_dict = {}
        self._domainid_hosts = {}
        self._datastore_hosts = {}
        self._guesthosts_datastore = {}

    def get_hostlist(self):
        return self._host_list

    def merge_dict(self, dict1, dict2):
        if not dict1:
            return dict2
        elif not dict2:
            return dict1
        elif type(dict1) is list and type(dict2) is list:
            for val in dict2:
                if val not in dict1:
                    dict1.append(val)
            return dict1
        elif type(dict1) is str and type(dict2) is str:
            if dict1 != dict2:
                dict1 += ", {0}".format(dict2)
            return dict1
        else:
            try:
                merged_dict = {}
                dict1_dup = {}
                dict2_dup = {}
                for key, val in dict1.iteritems():
                    if key not in dict2:
                        merged_dict.update({key: val})
                    else:
                        dict1_dup.update({key: val})
                for key, val in dict2.iteritems():
                    if key not in dict1:
                        merged_dict.update({key: val})
                    else:
                        dict2_dup.update({key: val})
                for key in dict1_dup.keys():
                    merged_dict.update(
                        {key: self.merge_dict(dict1_dup[key], dict2_dup[key])})
                return merged_dict
            except Exception:
                msg = "Merge dict is failed ( {0}.{1} )".format(
                    self.__class__.__name__,
                    self.merge_dict.__name__)
                raise dpclient.DbError(msg)

    def query_vm_relationship(self, vm_hosts, cond_dict):
        vm_datastore_list = []
        guestdomainid_datastore = {}

        relation_conn = Neo4jTool()
        vm_datastore = relation_conn.query_by_label_rels(
            vm_hosts,
            "VMHost", ["VmHostHasVmDatastore"],
            cond_dict,
            "name", "domainId")

        for host, info in vm_datastore.iteritems():
            for datastore in info["Datastores"].keys():
                if datastore not in vm_datastore_list:
                    vm_datastore_list.append(datastore)
                    self._datastore_hosts.update({datastore: [host]})
                else:
                    if host not in self._datastore_hosts[datastore]:
                        self._datastore_hosts[datastore].append(host)

        vm_domainid = relation_conn.query_by_label_rels(
            vm_datastore_list,
            "VMDatastore", ["VmVirtualMachineUsesVmDatastore"],
            cond_dict,
            "domainId", "domainId")

        vm_watchdog = []
        for host in vm_domainid.values():
            for vm in host["VMs"]:
                if vm not in vm_watchdog:
                    vm_watchdog.append(vm)
                    guestdomainid_datastore.update(
                        {vm: [host["Datastores"].keys()[0]]})
                else:
                    if host["Datastores"].keys()[0] not in guestdomainid_datastore[vm]:
                        guestdomainid_datastore[vm].append(
                            host["Datastores"].keys()[0])

        prediction_conn = InfluxdbTool()
        prediction_conn.set_display(["guest_hostname", "guest_ips",
                                     "name", "domain_id"])
        vmlist_result = prediction_conn.query_vm_host(
            "virtualmachine", vm_watchdog, ["domain_id"])

        for idx, host in enumerate(vmlist_result["guest_hostname"]):
            if vmlist_result["guest_ips"][idx]:
                if host:
                    vm_hostname = host[0]
                else:
                    vm_hostname = vmlist_result["guest_ips"][idx][0]
                vm_name = ''.join(vmlist_result["name"][idx])
                if vm_hostname not in self._ip_dict:
                    self._guesthosts_datastore.update({
                        vmlist_result["guest_hostname"][idx][0]: guestdomainid_datastore[vmlist_result["domain_id"][idx][0]]})
                    self._host_list.append(vm_hostname)
                    self._ip_dict.update({
                        vm_hostname: {"host_ip": [],
                                      "os_type": "vmware",
                                      "vm_name": vm_name}})
                target = self._ip_dict[vm_hostname]
                if vmlist_result["guest_ips"][idx][0] not in target["host_ip"]:
                    target["host_ip"].append(
                        vmlist_result["guest_ips"][idx][0])

        try:
            vm_disks = relation_conn.query_by_label_rels(
                vm_datastore_list,
                "VMDatastore", ["VmDatastoreComposesOfVmDisk"],
                cond_dict,
                "domainId", "domainId")
        except Exception:
            vm_disks = {}

        try:
            vsan_capacity = relation_conn.query_by_label_rels(
                vm_hosts,
                "VMHost", ["VmHostHasVmDiskGroup",
                           "VSanDiskGroupHasCapacityVmDisk"],
                cond_dict,
                "name", "domainId")
        except Exception:
            vsan_capacity = {}

        try:
            vsan_cache = relation_conn.query_by_label_rels(
                vm_hosts,
                "VMHost", ["VmHostHasVmDiskGroup",
                           "VSanDiskGroupHasCacheVmDisk"],
                cond_dict,
                "name", "domainId")
        except Exception:
            vsan_cache = {}

        try:
            vm_disks = self.merge_dict(vm_disks, vsan_capacity)
            vm_disks = self.merge_dict(vm_disks, vsan_cache)
        except Exception:
            pass

        return vm_disks

    def query_relationship(self, cond_dict):
        windows_hosts = []
        linux_hosts = []
        vm_hosts = []
        disk_relationship = {}
        windows_disks = {}
        vm_disks = {}

        relation_conn = Neo4jTool()
        host_dict = relation_conn.query_nodes(
            "VMHost", "name", self._host_list, cond_dict)

        for data in host_dict:
            if not self._host_list or data["name"] in self._host_list:
                self._domainid_hosts.update({data["domainId"]: data["name"]})

        self.query_iplist()

        for host in self._domainid_hosts.values():
            hostname = str(host)
            if self._ip_dict[hostname]["os_type"] and \
                    "windows" == self._ip_dict[hostname]["os_type"]:
                windows_hosts.append(hostname)
                if hostname not in self._host_list:
                    self._host_list.append(hostname)
            elif self._ip_dict[hostname]["os_type"] and \
                    "linux" == self._ip_dict[hostname]["os_type"]:
                linux_hosts.append(hostname)
                if hostname not in self._host_list:
                    self._host_list.append(hostname)
            elif self._ip_dict[hostname]["os_type"] and \
                    "vmware" == self._ip_dict[hostname]["os_type"]:
                vm_hosts.append(hostname)
                if hostname in self._host_list:
                    self._host_list.remove(hostname)
                self._ip_dict.pop(hostname)

        relation_conn = Neo4jTool()

        if windows_hosts:
            windows_disks.update(relation_conn.query_by_label_rels(
                windows_hosts,
                "VMHost", ["VmHostContainsVmDisk"],
                cond_dict,
                "name", "domainId"))
            disk_relationship.update(windows_disks)

        if vm_hosts:
            vm_disks = self.query_vm_relationship(vm_hosts, cond_dict)
            disk_relationship.update(vm_disks)

        return disk_relationship

    def query_iplist(self):
        for hostname in self._domainid_hosts.values():
            self._ip_dict.update(
                {hostname: {"host_ip": None, "os_type": None, "vm_name": None}})
        end_time = int(time.time() * 1000000000)
        start_time = end_time - (3600 * 12 * 1000000000)
        prediction_conn = InfluxdbTool()
        prediction_conn.set_display(["domain_id", "host_ip", "os_type"])
        result = prediction_conn.query_ip_by_host(
            [start_time, end_time], self._domainid_hosts.keys())

        result_dict = {}
        for key, val in result.iteritems():
            result_dict[self._domainid_hosts[key]] = result[key]

        for key, val in self._ip_dict.iteritems():
            if key in result_dict:
                val["host_ip"] = result_dict[key]["host_ip"][0].split(",") \
                    if result_dict[key]["host_ip"][0] \
                    else result_dict[key]["host_ip"][0]
                val["os_type"] = result_dict[key]["os_type"][0]

    def query_prediction(self, disk_watchdog):
        prediction_conn = InfluxdbTool()
        end_time = int(time.time() * 1000000000)
        start_time = end_time - (3600 * 12 * 1000000000)
        self._prediction_result = prediction_conn.query_pred_interval_by_disk([
            start_time, end_time], disk_watchdog)

        for key1 in self._prediction_result.keys():
            for key2 in self._prediction_result[key1].keys():
                last_time = 0
                if key2 == "None":
                    self._prediction_result[key1].pop(key2)
                    if not self._prediction_result[key1]:
                        self._prediction_result.pop(key1)
                    continue
                for key3 in self._prediction_result[key1][key2].keys():
                    if last_time == 0:
                        last_time = key3
                    elif key3 > last_time:
                        self._prediction_result[key1][key2].pop(last_time)
                        last_time = key3
                    else:
                        self._prediction_result[key1][key2].pop(key3)

    def query_diskinfo(self, disk_watchdog):
        diskinfo = {}
        type_dict = {
            "0": "Unknown",
            "1": "HDD",
            "2": "SSD",
            "3": "SSD NVME",
            "4": "SSD SAS",
            "5": "SSD SATA",
            "6": "HDD SAS",
            "7": "HDD SATA",
            "8": "SAS",
            "9": "SATA",
        }
        prediction_conn = InfluxdbTool()
        prediction_conn.set_display(["*", "disk_domain_id"])
        diskinfo_result = prediction_conn.query_disk_info(
            "sai_disk", disk_watchdog, ["disk_domain_id"])

        for key1 in diskinfo_result.keys():
            for key2 in diskinfo_result[key1].keys():
                for val in diskinfo_result[key1][key2].values():
                    disk_type = type_dict[val["disk_type"][0]]
                    '''
                    disk_name = "{0} {1}/{2} ({3},{4})".format(
                        ' '.join(val["vendor"]),
                        ' '.join(val["model"]),
                        ' '.join(val["serial_number"]),
                        disk_type,
                        ' '.join(val["disk_name"])).replace('"', '\\"')
                    '''
                    disk_name = "{0}".format(' '.join(val["disk_name"]))
                    diskinfo.update({key1: disk_name})
        return diskinfo

    def get_disklist(self):
        disk_watchdog = []
        disk_list = {}

        start_time = int(time.time() * 1000000000) - (3600 * 24 * 1000000000)
        cond_dict = {"time": start_time}

        result = self.query_relationship(cond_dict)

        try:
            for key1, val1 in result.iteritems():
                for key2, val2 in val1["Datastores"].iteritems():
                    for key3, val3 in val2["Disks"].iteritems():
                        if key1 in self._datastore_hosts:
                            host_domainid = self._datastore_hosts[key1][0]
                        else:
                            host_domainid = key1
                        if key3 not in disk_list:
                            disk_list.update({key3: [host_domainid]})
                            disk_watchdog.append(key3)
                        elif host_domainid not in disk_list[key3]:
                            disk_list[key3].append(host_domainid)
        except Exception:
            msg = "Neo4JApi Get wrong data ( {0}.{1} )".format(
                self.__class__.__name__,
                self.get_disklist.__name__)
            _logger.error("An error occurred when analysing DiskProphet data.")
            raise dpclient.DbError(msg)

        self.query_prediction(disk_watchdog)
        diskinfo = self.query_diskinfo(disk_watchdog)

        for key1, val1 in disk_list.iteritems():
            if key1 in self._prediction_result:
                for key2, val2 in self._prediction_result[key1].iteritems():
                    if key2 in self._domainid_hosts:
                        if self._domainid_hosts[key2] not in self._host_info:
                            self._host_info.update(
                                {self._domainid_hosts[key2]: {}})
                        target = self._host_info[self._domainid_hosts[key2]]
                        for key3, val3 in val2.iteritems():
                            if self._domainid_hosts[key2] not in self._host_status:
                                self._host_status.update(
                                    {self._domainid_hosts[key2]: val3["near_failure"][0]})
                            else:
                                if "Bad" == val3["near_failure"][0] or\
                                        ("Warning" == val3["near_failure"][0] and "Bad" != self._host_status[self._domainid_hosts[key2]]) or\
                                        ("Good" == val3["near_failure"][0] and "Bad" != self._host_status[self._domainid_hosts[key2]] and
                                         "Warning" != self._host_status[self._domainid_hosts[key2]]):
                                    if val3["near_failure"][0] != self._host_status[self._domainid_hosts[key2]]:
                                        self._host_status.update(
                                            {self._domainid_hosts[key2]: val3["near_failure"][0]})
                                        target.clear()
                            if val3["near_failure"][0] == self._host_status[self._domainid_hosts[key2]]:
                                if val3["disk_name"][0] not in target:
                                    target.update({val3["disk_name"][0]: {}})
                                target[val3["disk_name"][0]].update(
                                    {"near_failure": val3["near_failure"][0],
                                     "disk_name": diskinfo[key1]})

    def event_trigger(self):
        event_queue = []
        self.regist_events()
        self.get_disklist()
        for event in self.event_list:
            for self.target_host in self._ip_dict.keys():
                self.target_event = event
                variable = {}
                for idx in event.variable_extraction():
                    variable.update(
                        {idx: eval("self.get_{0}()".format(idx))})
                if None in variable.values():
                    continue
                if eval(event.condition_parser(variable)):
                    event.set_triggered(True)
                else:
                    event.set_triggered(False)
                event_queue.append(
                    [Event_obj(
                        event.get_event_id(),
                        event.get_display(),
                        event.get_severity(),
                        event.get_conditional(),
                        event.get_triggered()),
                     {self.target_host:
                      self._host_info[self.target_host] if self.target_host in self._host_info else
                      self.get_vm_host()},
                     self._ip_dict[self.target_host]])
        return event_queue

    def get_near_failure(self):
        try:
            if self.target_host in self._host_status:
                return self._host_status[self.target_host]
            else:
                self._nearfailure = None
                for datastore in self._guesthosts_datastore[
                        self.target_host]:
                    for host in self._datastore_hosts[datastore]:
                        if self._host_status[self._domainid_hosts[host]]:
                            status = self._host_status[self._domainid_hosts[host]]
                            if "Bad" == status or\
                                    ("Warning" == status and "Bad" != self._nearfailure) or\
                                    ("Good" == status and "Bad" != self._nearfailure and
                                     "Warning" != self._nearfailure):
                                self._nearfailure = status
                        if "Bad" == self._nearfailure:
                            return self._nearfailure
                return self._nearfailure
        except Exception:
            return None

    def get_host(self):
        if self.target_host in self.target_event.get_conditional():
            return self.target_host
        else:
            return None

    def get_vm_host(self):
        vm_host_info = {}
        for datastore in self._guesthosts_datastore[
                self.target_host]:
            for host in self._datastore_hosts[datastore]:
                if self._host_info[self._domainid_hosts[host]]:
                    target = self._host_info[self._domainid_hosts[host]]
                    for diskinfo in target.values():
                        if diskinfo["near_failure"] == self._nearfailure:
                            vm_host_info.update(target)
        return vm_host_info


class Neo4jTool(dpclient.Neo4jApi):
    """
    DiskProphet connector V1 API
    """

    def __init__(self):
        super(Neo4jTool, self).__init__()

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
            _logger.error("Get no results from DiskProphet.")
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
            _logger.error("Get no results from DiskProphet.")
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
            _logger.error("Get no results from DiskProphet.")
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

    def __init__(self):
        super(InfluxdbTool, self).__init__()

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
        except dpclient.DbError:
            msg = "InfluxdbApi Get no results ( {0}.{1} )" \
                " Your URL is {2}".format(
                    self.__class__.__name__,
                    self.query_data_detail_setup.__name__, self.url)
            _logger.error("Get no results from DiskProphet.")
            raise dpclient.DbError(msg)

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
            _logger.error("Get no results from DiskProphet.")
            raise dpclient.DbError(msg)

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