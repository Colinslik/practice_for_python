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

import dpclient
from ifttt.define import validate_ifttt_request
from res.auth import auth
import workloadevent


class WorkloadMetrics(Resource):
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

        super(WorkloadMetrics, self).__init__()

    def get_workload_data(self, filter_dict):
        workload_data = []
        try:
            workloadevent_conn = workloadevent.WorkloadEvent('*')

            if filter_dict:
                workloadevent_conn.setup_event_filter(filter_dict)

            workload_result = workloadevent_conn.event_trigger()

            if workload_result:
                for data in workload_result:
                    timestamp = int(data["time"]) / 1000000000
                    created_at = datetime.fromtimestamp(
                        timestamp
                    ).isoformat() + 'Z'

                    workload_data.append({
                        "date": created_at,
                        "event_type": data["event_type"],
                        "event_level": data["event_level"],
                        "title": data["title"],
                        "agent_host": data["agenthost"],
                        "meta": {
                            "id": data["time"],
                            "timestamp": timestamp
                        }})
        except dpclient.DbError as e:
            print e.msg

        return workload_data

    @validate_ifttt_request
    def post(self):
        # post
        try:
            args = self.reqparse.parse_args()
            limit = -1 if args.get('limit') is None else args.get('limit')
            filter_dict = None if not args.get('triggerFields') \
                else args.get('triggerFields')

            if filter_dict["type"] and filter_dict["level"]:
                raw = self.get_workload_data(filter_dict)
                data = raw
                if limit >= 0:
                    data = raw[:limit]

                body = json.dumps(
                    {'data': data}, ensure_ascii=False).decode('utf-8')
                resp = make_response(
                    body, OK,
                    {"Content-Type": "application/json; charset=utf-8"})
            else:
                raise
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
    test1 = WorkloadMetrics()
    print test1.get_workload_data({
        "type": "Metrics Alert",
        "level": "Warn"
    })
