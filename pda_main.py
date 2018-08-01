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
    for i in action_list:
        for key, val in action_dict.iteritems():
            if key in i[0]:
                val.append(i)


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
                           "action.noop": []}

            _logger.info("Service is running...")

            host_list = re.split(
                ' ,|, | |,',
                _conf.get('service.arcserve', 'hosts.registered', '*'))

            diskevent_conn = diskevent.DiskEvent(host_list)
            policy_conn = policymanager.PolicyManager(
                diskevent_conn.event_trigger())
            host_watchdog = diskevent_conn.get_hostlist()

            action_distributor(policy_conn.policy_trigger(), action_dict)

            logalert_conn = logalert.LogAlert()
            logalert_conn.dump_log(action_dict["action.log"])

            arcservecmd_conn = arcservecmd.ArcserveApi("cmd")
            arcservecmd_conn.run_cmd(action_dict["action.cmd.arcserve"])

            arcservecmd_conn = arcservecmd.ArcserveApi("ps")
            arcservecmd_conn.run_cmd(action_dict["action.ps.arcserve"])

            rawcmd_conn = rawcmd.RawCmdApi("cmd")
            rawcmd_conn.noop_action(action_dict["action.noop"])
            rawcmd_conn.run_cmd(action_dict["action.cmd.raw"])

            rawcmd_conn = rawcmd.RawCmdApi("ps")
            rawcmd_conn.run_cmd(action_dict["action.ps.raw"])

            mailalert_conn = mailalert.MailAlert()
            mailalert_conn.send_email(action_dict["action.email"])

            smsalert_conn = smsalert.SmsAlert()
            smsalert_conn.send_message(action_dict["action.sms.twilio"])

            arcserve_info_collector = arcservecmd.ArcserveApi()
            pda_api.update_backups(
                arcserve_info_collector.get_arcserve_info(host_watchdog))

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
