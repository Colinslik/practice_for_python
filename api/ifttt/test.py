#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from httplib import NOT_FOUND, BAD_REQUEST
from httplib import OK
import json

from flask import make_response
from flask_restful import Resource
from flask_restful import reqparse

from ifttt.define import validate_ifttt_request
from res.auth import auth


class Test(Resource):
    #decorators = [auth.login_required]

    def __init__(self):
        super(Test, self).__init__()

    @validate_ifttt_request
    def post(self):
        # get
        data = {
            "accessToken": "6F5996AEAAEE6DFF8BCA25D599265",
            "samples": {
                "triggers": {
                    "diskpredict": {
                        "status": "Bad"
                    },
                    "workloadmetrics": {
                        "type": "Metrics Alert",
                        "level": "Warn"
                    }
                },
                "actions": {
                    "create_new_thing": {
                    }
                },
                "actionRecordSkipping": {
                    "create_new_thing": {
                    }
                }
            }
        }

        body = json.dumps({'data': data}, ensure_ascii=False).decode('utf-8')
        resp = make_response(
            body, OK, {"Content-Type": "application/json; charset=utf-8"})
        return resp
