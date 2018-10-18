#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from datetime import datetime
from httplib import NOT_FOUND, BAD_REQUEST
from httplib import OK
import json
import time
import uuid

from flask import make_response
from flask_restful import Resource
from flask_restful import reqparse

import diskevent
import dpclient
from ifttt.define import validate_ifttt_request
from res.auth import auth


class DiskPredict(Resource):
    #decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument(
            'trigger_identity', type=str, location='json')
        self.reqparse.add_argument(
            'triggerFields', type=dict, location='json')
        self.reqparse.add_argument('user', type=dict, location='json')
        self.reqparse.add_argument('fttt_source', type=dict, location='json')
        self.reqparse.add_argument('limit', type=int, location='json')

        super(DiskPredict, self).__init__()

    def get_prediction_data(self, filter_status):
        prediction_data = []
        try:
            diskevent_conn = diskevent.DiskEvent('*')
            prediction_result = diskevent_conn.event_trigger()

            if prediction_result:
                for hostname, info in prediction_result.iteritems():
                    for diskname, diskstatus in info.iteritems():
                        status = diskstatus["near_failure"]
                        if status.lower() != "good" and \
                                (not filter_status or
                                 status.lower() == filter_status):
                            timestamp = int(diskstatus["time"]) / 1000000000
                            created_at = datetime.fromtimestamp(
                                timestamp
                            ).isoformat() + 'Z'

                            prediction_data.append({
                                "created_at": created_at,
                                "host": hostname,
                                "disk": diskname,
                                "state": status,
                                "meta": {
                                    "id": diskstatus["time"],
                                    "timestamp": timestamp
                                }})
        except dpclient.DbError as e:
            print e.msg

        return prediction_data

    @validate_ifttt_request
    def post(self):
        # post
        try:
            args = self.reqparse.parse_args()
            limit = -1 if args.get('limit') is None else args.get('limit')
            filter_dict = None if not args.get('triggerFields') \
                else args.get('triggerFields')

            if filter_dict["status"] == "default":
                filter_status = None
            else:
                filter_status = filter_dict["status"].lower()

            raw = self.get_prediction_data(filter_status)

            data = raw
            if limit >= 0:
                data = raw[:limit]

            body = json.dumps(
                {'data': data}, ensure_ascii=False).decode('utf-8')
            resp = make_response(
                body, OK, {"Content-Type": "application/json; charset=utf-8"})

        except Exception:
            body = json.dumps(
                {'errors': [
                    {'message': 'Missing required parameter in the JSON body'}]
                 },
                ensure_ascii=False).decode('utf-8')
            resp = make_response(
                body, BAD_REQUEST,
                {"Content-Type": "application/json; charset=utf-8"})

        return resp


if __name__ == '__main__':
    test1 = DiskPredict()
    print test1.get_prediction_data(None)
