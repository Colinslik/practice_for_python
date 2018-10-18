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
    class Event_Manager(object):
        def __init__(self):
            pass


class DiskEvent(Event_Manager):

    def __init__(self, host_list, host="127.0.0.1"):
        super(DiskEvent, self).__init__()
        self.host = host
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

    def get_vm_dict(self):
        vm_name_dict = {}
        for hostname, info in self._ip_dict.iteritems():
            if info["vm_name"]:
                vm_name_dict.update(
                    {str(hostname.lower()): str(info["vm_name"].lower())})
        return vm_name_dict

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

        relation_conn = dpclient.Neo4jTool(self.host)
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

        # update vsan info
        vm_datastore_info = relation_conn.query_nodes(
            "VMDatastore", "domainId", vm_datastore_list, cond_dict)

        for node_info in vm_datastore_info:
            if int(node_info["isVsan"]):
                self._datastore_hosts.update(
                    {node_info["domainId"]: [str(node_info["domainId"])]})
                self._domainid_hosts.update(
                    {node_info["domainId"]: node_info["name"]})

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

        prediction_conn = dpclient.InfluxdbTool(self.host)
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
                vm_datastore_list,
                "VMDatastore", ["VsanDatastoreContainsVmDiskGroup",
                                "VSanDiskGroupHasCapacityVmDisk"],
                cond_dict,
                "domainId", "domainId")
        except Exception:
            vsan_capacity = {}

        try:
            vsan_cache = relation_conn.query_by_label_rels(
                vm_datastore_list,
                "VMDatastore", ["VsanDatastoreContainsVmDiskGroup",
                                "VSanDiskGroupHasCacheVmDisk"],
                cond_dict,
                "domainId", "domainId")
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

        relation_conn = dpclient.Neo4jTool(self.host)
        host_dict = relation_conn.query_nodes(
            "VMHost", "name", self._host_list, cond_dict)

        for data in host_dict:
            if not self._host_list or data["name"] in self._host_list:
                self._domainid_hosts.update({data["domainId"]: data["name"]})

        self._ip_dict = self.query_iplist()

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

        relation_conn = dpclient.Neo4jTool(self.host)

        if windows_hosts:
            windows_disks.update(relation_conn.query_by_label_rels(
                windows_hosts,
                "VMHost", ["VmHostContainsVmDisk"],
                cond_dict,
                "name", "domainId"))
            disk_relationship.update(windows_disks)

    #        if vm_hosts:
    #            vm_disks = self.query_vm_relationship(vm_hosts, cond_dict)
    #            disk_relationship.update(vm_disks)

        return disk_relationship

    def query_iplist(self, domainid_hosts=None):
        ip_dict = {}

        if not domainid_hosts:
            domainid_hosts = self._domainid_hosts

        for hostname in domainid_hosts.values():
            ip_dict.update(
                {hostname: {"host_ip": None, "os_type": None, "vm_name": None}})
        end_time = int(time.time() * 1000000000)
        start_time = end_time - (3600 * 12 * 1000000000)
        prediction_conn = dpclient.InfluxdbTool(self.host)
        prediction_conn.set_display(["domain_id", "host_ip", "os_type"])
        result = prediction_conn.query_ip_by_host(
            [start_time, end_time], domainid_hosts.keys())

        result_dict = {}
        for key, val in result.iteritems():
            result_dict[domainid_hosts[key]] = result[key]

        for key, val in ip_dict.iteritems():
            if key in result_dict:
                val["host_ip"] = result_dict[key]["host_ip"][0].split(",") \
                    if result_dict[key]["host_ip"][0] \
                    else result_dict[key]["host_ip"][0]
                val["os_type"] = result_dict[key]["os_type"][0]

        return ip_dict

    def query_prediction(self, disk_watchdog):
        prediction_conn = dpclient.InfluxdbTool(self.host)
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
        prediction_conn = dpclient.InfluxdbTool(self.host)
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
            if not ifttt:
                _logger.error(
                    "An error occurred when analysing DiskProphet data.")
            else:
                print "An error occurred when analysing DiskProphet data."
            raise dpclient.DbError(msg)

        self.query_prediction(disk_watchdog)
        diskinfo = self.query_diskinfo(disk_watchdog)

        for key1, val1 in disk_list.iteritems():
            if key1 in self._prediction_result:
                for val2 in self._prediction_result[key1].values():
                    if val1[0] in self._domainid_hosts:
                        if self._domainid_hosts[val1[0]] not in self._host_info:
                            self._host_info.update(
                                {self._domainid_hosts[val1[0]]: {}})
                        target = self._host_info[self._domainid_hosts[val1[0]]]
                        for key3, val3 in val2.iteritems():
                            if self._domainid_hosts[val1[0]] not in self._host_status:
                                self._host_status.update(
                                    {self._domainid_hosts[val1[0]]: val3["near_failure"][0]})
                            else:
                                if "Bad" == val3["near_failure"][0] or\
                                        ("Warning" == val3["near_failure"][0] and "Bad" != self._host_status[self._domainid_hosts[val1[0]]]) or\
                                        ("Good" == val3["near_failure"][0] and "Bad" != self._host_status[self._domainid_hosts[val1[0]]] and
                                         "Warning" != self._host_status[self._domainid_hosts[val1[0]]]):
                                    if val3["near_failure"][0] != self._host_status[self._domainid_hosts[val1[0]]]:
                                        self._host_status.update(
                                            {self._domainid_hosts[val1[0]]: val3["near_failure"][0]})
                                        target.clear()
                            if val3["near_failure"][0] == self._host_status[self._domainid_hosts[val1[0]]]:
                                if val3["disk_name"][0] not in target:
                                    target.update({val3["disk_name"][0]: {}})
                                if not ifttt:
                                    target[val3["disk_name"][0]].update(
                                        {"near_failure": val3["near_failure"][0],
                                         "disk_name": diskinfo[key1]})
                                else:
                                    target[val3["disk_name"][0]].update(
                                        {"near_failure": val3["near_failure"][0],
                                         "disk_name": diskinfo[key1],
                                         "time": key3})

    if not ifttt:
        def event_trigger(self):
            event_queue = []
            self.regist_events()
            self.get_disklist()
            for event in self.event_list:
                for self.target_host, self.target_info \
                        in self._ip_dict.iteritems():
                    self.target_event = event
                    variable = {}
                    for idx in event.variable_extraction():
                        variable.update(
                            {idx: eval("self.get_{0}()".format(idx))})
                    if None in variable.values():
                        continue
                    if eval(event.condition_parser(variable).lower()):
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
                          self._host_info[self.target_host] if self.target_host
                          in self._host_info else
                          self.get_vm_host()},
                         self._ip_dict[self.target_host]])
            return event_queue
    else:
        def event_trigger(self):
            host_info_dict = {}
            target_status = {"": []}
            self.get_disklist()

            for hostname, hoststatus in self._host_status.iteritems():
                if hoststatus != target_status.keys()[0]:
                    if "Bad" == hoststatus or\
                        ("Warning" == hoststatus and "Bad" != target_status.keys()[0]) or\
                        ("Good" == hoststatus and "Bad" != target_status.keys()[0] and
                         "Warning" != target_status.keys()[0]):
                        target_status = {hoststatus: [hostname]}
                else:
                    target_status[hoststatus].append(hostname)

            for hostname in target_status.values()[0]:
                for diskname, diskinfo in self._host_info[hostname].iteritems():
                    if diskinfo["near_failure"] == target_status.keys()[0]:
                        if hostname not in host_info_dict:
                            host_info_dict[hostname] = {}
                        host_info_dict[hostname].update(
                            {diskname: {"near_failure": diskinfo["near_failure"],
                                        "time": diskinfo["time"]}})

            return host_info_dict

    if not ifttt:
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
            if self.target_host.lower() in \
                    self.target_event.get_conditional().lower():
                return self.target_host
            elif "{vm_name}" in self.target_event.get_conditional():
                return ""
            else:
                return None

        def get_vm_name(self):
            if self.target_info["vm_name"] and \
                self.target_info["vm_name"].lower() in \
                    self.target_event.get_conditional().lower():
                return self.target_info["vm_name"]
            elif "{host}" in self.target_event.get_conditional():
                return ""
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


if __name__ == '__main__':
    if ifttt:
        try:
            diskevent_conn = DiskEvent('*')
            prediction_result = diskevent_conn.event_trigger()

            if prediction_result:
                for hostname, info in prediction_result.iteritems():
                    for diskname, diskstatus in info.iteritems():
                        print "host:{0}   disks:{1}   status:{2}  time:{3}".format(
                            hostname, diskname,
                            diskstatus["near_failure"], diskstatus["time"])
        except dpclient.DbError as e:
            print e.msg
