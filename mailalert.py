# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.header import Header
from httplib import OK, BAD_REQUEST, INTERNAL_SERVER_ERROR, UNAUTHORIZED, SERVICE_UNAVAILABLE
import smtplib

import config
from eventmanager import EventDb
import logger


class MailError(Exception):
    def __init__(self, msg):
        self.msg = msg


# Get logger
_logger = logger.get_logger()

# Get configuration
_conf = config.get_config()


class MailAlert(object):
    """
    Email Alert V1
    """

    def __init__(self):
        global _conf
        _conf = config.get_config()

        self.smtp = self.get_smtp()
        self.mail_format = self.get_mail_format()
        self.mail_dict = {}

    def get_smtp(self):
        server = _conf.get('main', 'smtp.host', None)
        port = _conf.getint('main', 'smtp.port', None)
        security = _conf.get('main', 'smtp.security', None)
        user = _conf.get('main', 'smtp.username', None)
        password = _conf.get('main', 'smtp.password', None)

        if server and port and security and user and password:
            return {'server': server,
                    'port': port,
                    'security': security,
                    'user': user,
                    'password': password}
        else:
            return None

    def get_mail_format(self):
        mail_dict = {}
        for i in _conf.sections():
            if i.startswith("action.email"):
                mail_dict.update({i: {}})
                target = mail_dict[i]
                for (key, val) in _conf.items(i):
                    target.update({key: val})
        return mail_dict

    def get_mail_dict(self, send_list):
        for mail_data in send_list:
            if mail_data[0] not in self.mail_dict:
                self.mail_dict.update(
                    {mail_data[0]: {}})
            target_host = self.mail_dict[mail_data[0]]
            if mail_data[3].keys()[0] not in target_host:
                target_host.update(
                    {
                        mail_data[3].keys()[0]: {
                            "disks": [],
                            "display": mail_data[2]["display"]}})
            target = target_host[mail_data[3].keys()[0]]
            for disk_list in mail_data[3].values():
                for disk, status in disk_list.iteritems():
                    target["disks"].append({disk: {"near_failure": status["near_failure"],
                                                   "disk_name": status["disk_name"]}})

    def send_email(self, send_list):
        if not self.smtp:
            if send_list:
                _logger.warning("[MailAlert] SMTP info is not sufficient.")
                eventdb_conn = EventDb()
                for mail_data in send_list:
                    eventdb_conn.push_queue(mail_data[4])
                eventdb_conn.db_streaming()
            return None
        try:
            self.get_mail_dict(send_list)
            for mail_data in send_list:
                msg = MIMEMultipart()
                msg['To'] = self.mail_format[mail_data[0]]["to"]
                msg['From'] = self.mail_format[mail_data[0]]["from"]
                msg['CC'] = self.mail_format[mail_data[0]]["cc"]
                subject = self.mail_format[mail_data[0]]["subject"].format(
                    **self.mail_dict[mail_data[0]][mail_data[3].keys()[0]])
                msg['Subject'] = "%s" % Header(subject, 'utf-8')

                disk_status = ""
                for disk in self.mail_dict[
                    mail_data[0]][
                        mail_data[3].keys()[0]]["disks"]:
                    for status in disk.values():
                        disk_status += "{0} ({1}), ".format(
                            status["disk_name"], status["near_failure"])

                replaced = {"host": mail_data[3].keys()[0],
                            "disks": disk_status[:-2]}
                body = self.mail_format[mail_data[0]]["body"].format(
                    **replaced)
                msg.attach(MIMEText(body, 'plain', 'UTF-8'))

                server = SMTPMailer(self.smtp)

                from_addr = msg['From']
                to_addrs = msg['To'].split(
                    ',') + msg['CC'].split(',') + \
                    self.mail_format[mail_data[0]
                                     ]["bcc"].split(',')  # list
                retcode = server.send(from_addr, to_addrs, msg.as_string())

                if retcode == OK:
                    _logger.info(
                        "[MailAlert] Successfully sent email.")
                    mail_data[4]["status"] = mail_data[4]["status_change"]
                else:
                    _logger.error("[MailAlert] Failed to send mail.")
                    _logger.debug("retcode %d" % (retcode))
        except Exception as e:
            _logger.error("[MailAlert] Failed to send mail.")
            _logger.debug("%s" % (e))
        eventdb_conn = EventDb()
        for mail_data in send_list:
            eventdb_conn.push_queue(mail_data[4])
        eventdb_conn.db_streaming()


class SMTPMailer(object):

    """Federator SMTP Mailer Class"""

    smtp = None
    server = None

    def __init__(self, smtp=None):
        if not smtp:
            # Get SMTP setting from configuration
            global _conf
            _conf = config.get_config()

            smtp = {}
            smtp['server'] = _conf.get('main', 'smtp.host', 'localhost')
            smtp['port'] = _conf.getint('main', 'smtp.port', 25)
            smtp['security'] = _conf.get('main', 'smtp.security', None)
            smtp['user'] = _conf.get('main', 'smtp.username', None)
            smtp['password'] = _conf.get('main', 'smtp.password', None)

        self.smtp = smtp

    def __del__(self):
        if self.server:
            self.server.close()

    def connect(self):
        retcode = OK

        if self.server:
            self.server.close()
            self.server = None

        try:
            if self.smtp.get('security') == 'SSL':
                self.server = smtplib.SMTP_SSL(
                    host=self.smtp['server'], port=self.smtp['port'], timeout=30)
            else:
                self.server = smtplib.SMTP(
                    host=self.smtp['server'], port=self.smtp['port'], timeout=30)

            self.server.ehlo()
            if self.smtp.get('security') == 'STARTTLS':
                self.server.starttls()
                self.server.ehlo()

        except Exception as e:
            retcode = SERVICE_UNAVAILABLE
            _logger.error(
                "[MailAlert] Failed to connect to SMTP server (server: '%s', port: '%s')."
                % (self.smtp['server'], self.smtp['port']))
            _logger.debug("%s" % (e))

        return retcode

    def hello(self):
        retcode = OK

        # test connection
        if self.server:
            try:
                self.server.ehlo()
            except smtplib.SMTPServerDisconnected as e:
                # reconnect
                retcode = self.connect()
                _logger.error(
                    "[MailAlert] Failed to connect to SMTP server (server: '%s', port: '%s')."
                    % (self.smtp['server'], self.smtp['port']))
                _logger.debug("%s" % (e))

        else:
            retcode = self.connect()

        if self.server and retcode == OK:
            try:
                # login
                if self.smtp.get('user') and self.smtp.get('password'):
                    self.server.login(self.smtp['user'], self.smtp['password'])

            except smtplib.SMTPAuthenticationError as e:
                _logger.error(
                    "[MailAlert] User '%s' failed to login."
                    % self.smtp['user'])
                _logger.debug("%s" % (e))
                retcode = UNAUTHORIZED

            except Exception as e:
                _logger.error(
                    "[MailAlert] User '%s' failed to login."
                    % self.smtp['user'])
                _logger.debug("%s" % (e))
                retcode = INTERNAL_SERVER_ERROR

        return retcode

    def send(self, from_addr, to_addrs, msg):

        if not from_addr or not to_addrs or not msg:
            return BAD_REQUEST

        retcode = self.hello()

        if retcode == OK:
            try:
                self.server.sendmail(from_addr, to_addrs, msg)

            except Exception as e:
                _logger.error("[MailAlert] Failed to send mail.")
                _logger.debug("%s" % (e))
                retcode = INTERNAL_SERVER_ERROR

        return retcode


if __name__ == '__main__':
    smtp = {}
    smtp['server'] = "smtp.gmail.com"
    smtp['port'] = 587
    smtp['security'] = "STARTTLS"
    smtp['user'] = ""
    smtp['password'] = ""

    server = SMTPMailer(smtp)

    msg = MIMEText("This is a test1.")
    msg['Subject'] = "Test Mail from gmail port 587"
    msg['From'] = "sender@gmail.com"
    msg['To'] = "user1@prophetstor.com"
    msg['Cc'] = ""

    from_addr = msg['From']
    to_addrs = msg['To'].split(',') + msg['Cc'].split(',')  # list
    server.send(from_addr, to_addrs, msg.as_string())

    smtp['server'] = "smtp.gmail.com"
    smtp['port'] = 465
    smtp['security'] = "SSL"
    smtp['user'] = ""
    smtp['password'] = ""

    msg = MIMEText("This is a test2.")
    msg['Subject'] = "Test Mail from gmail port 465"
    msg['From'] = "sender@gmail.com"
    msg['To'] = "user1@prophetstor.com"
    msg['Cc'] = "user2@prophetstor.com"

    from_addr = msg['From']
    to_addrs = msg['To'].split(',') + msg['Cc'].split(',')  # list
    server.send(from_addr, to_addrs, msg.as_string())

    smtp = {}
    smtp['server'] = "smtp.office365.com"
    smtp['port'] = 587
    smtp['security'] = "STARTTLS"
    smtp['user'] = ""
    smtp['password'] = ""

    server = SMTPMailer(smtp)

    msg = MIMEText("This is a test3.")
    msg['Subject'] = "Test Mail form office365"
    msg['From'] = "sender@prophetstor.com"
    msg['To'] = "user1@prophetstor.com"
    msg['Cc'] = "user2@prophetstor.com"
    bcc = ["user3@prophetstor.com"]

    from_addr = msg['From']
    to_addrs = msg['To'].split(',') + msg['Cc'].split(',') + bcc  # list
    server.send(from_addr, to_addrs, msg.as_string())
