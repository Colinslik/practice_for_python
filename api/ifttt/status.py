#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from httplib import OK
from flask_restful import Resource

from ifttt.define import validate_ifttt_request


class Status(Resource):

    def __init__(self):
        super(Status, self).__init__()

    @validate_ifttt_request
    def get(self):
        # get
        return {}, OK
