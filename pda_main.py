#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#
"""
ProphetStor Predictive Data Adapter
"""
# Get configuration and logger

import datetime
from httplib import OK
import re
import sys
import time
import traceback

import arcservecmd
import config
import define
import diskevent
from dpclient import ConfigError
from dpclient import DbError
import drprophetcmd
import logalert
import logger
import mailalert
from pdaclient import PdaApi
import policymanager
import rawcmd
import smsalert


_conf = config.get_config()
_logger = logger.get_logger()


def action_distributor(action_list, action_dict):
    action_list.sort()
    priority_key = 0
    for data in action_list:
        priority_key = data.keys()[0]
        for key, val in action_dict.iteritems():
            if key in data.values()[0][0]:
                if len(val) != priority_key:
                    target = []
                    val.insert(0, target)
                target.append(data.values()[0])


def merge_dictionary(dict1, dict2):
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
                    {key: merge_dictionary(dict1_dup[key], dict2_dup[key])})
            return merged_dict
        except Exception:
            _logger.error("Merge dict is failed.")
            return dict1


def main():
    global _conf
    last_time = 0
    conf_modify_time = 0
    reload_time = 6
    while True:
        pda_api = PdaApi()
        try:
            while True:
                # update status of PDA service
                status, data = pda_api.update_instance()
                if status == OK:
                    # get last modified time of configuration
                    api_conf_time = data.get('configs', {}).get(
                        'pda.conf.user', {}).get('lastUpdated', 0)

                    if api_conf_time > conf_modify_time:
                        # download new configuration
                        pda_api.download_config()
                        conf_modify_time = api_conf_time

                        dt = datetime.datetime.fromtimestamp(conf_modify_time)
                        _logger.info("Configuration downloaded: %s" %
                                     dt.strftime('%Y-%m-%d %H:%M:%S'))

                # reload configuration

                try:
                    if _conf.isUpdated():
                        _conf = config.reload_config()
                        logger.reload_debuglevel()
                        dt = datetime.datetime.fromtimestamp(_conf.modify_time)
                        _logger.info("Configuration modified: %s" %
                                     dt.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception:
                    _logger.error("Check config file error.")
                else:
                    enabled = _conf.getboolean('main', 'main.enabled', False)
                    if enabled:
                        interval = _conf.getint('main', 'main.interval', 3600)
                        reload_time = _conf.getint(
                            'main', 'main.interval.reload', 60)
                        current_time = time.time()
                        next_time = last_time + interval
                        if current_time >= next_time:
                            last_time = current_time
                            break

                time.sleep(reload_time)

            retcode = 0
            action_dict = {"action.email": [],
                           "action.sms.twilio": [],
                           "action.cmd.arcserve": [],
                           "action.ps.arcserve": [],
                           "action.log": [],
                           "action.cmd.raw": [],
                           "action.ps.raw": [],
                           "action.drprophet": [],
                           "action.noop": []}

            _logger.info("Service is running...")

            host_list = re.split(
                ' ,|, | |,',
                _conf.get('service.arcserve', 'hosts.registered', '*'))

            diskevent_conn = diskevent.DiskEvent(host_list)
            policy_conn = policymanager.PolicyManager(
                diskevent_conn.event_trigger())
            host_watchdog = diskevent_conn.get_hostlist()
            vm_name_dict = diskevent_conn.get_vm_dict()

            action_distributor(policy_conn.policy_trigger(), action_dict)

            logalert_conn = logalert.LogAlert()
            for act_data in action_dict["action.log"]:
                logalert_conn.dump_log(act_data)

            arcservecmd_conn = arcservecmd.ArcserveApi("cmd")
            for act_data in action_dict["action.cmd.arcserve"]:
                arcservecmd_conn.run_cmd(act_data)

            arcservecmd_conn = arcservecmd.ArcserveApi("ps")
            for act_data in action_dict["action.ps.arcserve"]:
                arcservecmd_conn.run_cmd(act_data)

            drprophetcmd_conn = drprophetcmd.DRProphetApi()
            for act_data in action_dict["action.drprophet"]:
                drprophetcmd_conn.run_cmd(act_data)

            rawcmd_conn = rawcmd.RawCmdApi("cmd")
            for act_data in action_dict["action.noop"]:
                rawcmd_conn.noop_action(act_data)
            for act_data in action_dict["action.cmd.raw"]:
                rawcmd_conn.run_cmd(act_data)

            rawcmd_conn = rawcmd.RawCmdApi("ps")
            for act_data in action_dict["action.ps.raw"]:
                rawcmd_conn.run_cmd(act_data)

            mailalert_conn = mailalert.MailAlert()
            for act_data in action_dict["action.email"]:
                mailalert_conn.send_email(act_data)

            smsalert_conn = smsalert.SmsAlert()
            for act_data in action_dict["action.sms.twilio"]:
                smsalert_conn.send_message(act_data)

            try:
                arcserve_info_collector = arcservecmd.ArcserveApi()
                arcserve_info = arcserve_info_collector.get_arcserve_info(
                    host_watchdog)
            except Exception:
                arcserve_info = {}

            try:
                drprophet_info_collector = drprophetcmd.DRProphetApi()
                drprophet_info = drprophet_info_collector.get_drprophet_info(
                    host_watchdog, vm_name_dict)
            except Exception:
                drprophet_info = {}

            merged_backup_info = merge_dictionary(
                arcserve_info, drprophet_info)
            pda_api.update_backups(merged_backup_info)

        except DbError as e:
            _logger.debug("%s" % (e.msg))

        except ConfigError as e:
            _logger.debug("%s" % (e.msg))

        except WindowsError as e:
            _logger.error("Run command failed")
            _logger.debug("result: %s" % (e))

        except Exception:
            retcode = 1
            _logger.exception(define.PRODUCT_NAME)
            print traceback.format_exc()

        time.sleep(10)

    sys.exit(retcode)


if __name__ == '__main__':
    main()
