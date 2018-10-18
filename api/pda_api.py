#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

from httplib import OK, NOT_FOUND

from flask import Flask, jsonify
from flask_restful import Api

from ifttt.diskpredict import DiskPredict
from ifttt.status import Status
from ifttt.test import Test
from ifttt.workloadmetrics import WorkloadMetrics
from res.action import Action, ActionList
from res.backup import Backup
from res.config import Config, ConfigList
from res.event import Event, EventList
from res.host import HostList, HostCorelation, HostBackup
from res.instance import Instance, InstanceList
#from res.job import JobList
from res.pdajob import PDAJobList
from res.policy import Policy, PolicyList
from res.setting import Setting
from res.user import User


app = Flask(__name__)
api = Api(app)


@app.route('/')
def root():
    response = {"api": "ProphetStor Predictive Data Adapter API",
                "version": "1.0.0",
                "status": "running",
                "message": "Welcome to access PDA API."}
    return jsonify(response), OK


@app.errorhandler(404)
def not_found(e):
    response = {"message": "Resource not found."}
    return jsonify(response), NOT_FOUND


# add api resources
api.add_resource(ConfigList, '/<string:host>/configs',
                 '/<string:host>/configs/')
api.add_resource(Config, '/<string:host>/configs/<string:id>')
api.add_resource(Setting, '/<string:host>/settings/<string:id>')
api.add_resource(User, '/users/<string:id>')
api.add_resource(InstanceList, '/instances', '/instances/')
api.add_resource(Instance, '/instances/<string:id>')
api.add_resource(HostList, '/<string:host>/hosts', '/<string:host>/hosts/')
api.add_resource(HostCorelation, '/<string:host>/hosts/<string:application>/corelation',
                 '/<string:host>/hosts/<string:application>/corelation/')
api.add_resource(HostBackup, '/<string:host>/hosts/<string:application>/backup',
                 '/<string:host>/hosts/<string:application>/backup/')
#api.add_resource(JobList, '/<string:host>/jobs', '/<string:host>/jobs/')
api.add_resource(PDAJobList, '/<string:host>/jobs', '/<string:host>/jobs/')
api.add_resource(PolicyList, '/<string:host>/policies',
                 '/<string:host>/policies/')
api.add_resource(Policy, '/<string:host>/policies/<string:id>')
api.add_resource(EventList, '/<string:host>/events', '/<string:host>/events/')
api.add_resource(Event, '/<string:host>/events/<string:id>')
api.add_resource(ActionList, '/<string:host>/actions',
                 '/<string:host>/actions/')
api.add_resource(Action, '/<string:host>/actions/<string:id>')
api.add_resource(Backup, '/<string:host>/backups/<string:application>')

# add api resources for IFTTT
api.add_resource(Status, '/ifttt/v1/status')
api.add_resource(Test, '/ifttt/v1/test/setup')
api.add_resource(DiskPredict, '/ifttt/v1/triggers/diskpredict')
api.add_resource(WorkloadMetrics, '/ifttt/v1/triggers/workloadmetrics')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8345)
