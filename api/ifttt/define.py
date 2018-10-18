#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import json
from httplib import UNAUTHORIZED
from flask import request, abort, make_response
from functools import wraps

SERVICE_KEY = "n3oNJ8CE6FIJiMHQuCD15VTRCm3d2VMvLZwNocUHCP6EeRMopSNSPYlqtG0JAWj2"
CHANNEL_KEY = "n3oNJ8CE6FIJiMHQuCD15VTRCm3d2VMvLZwNocUHCP6EeRMopSNSPYlqtG0JAWj2"


def validate_ifttt_request(f):
    """Validate the incoming request is from IFTTT"""
    @wraps(f)
    def decorated_function(*args, **kwargs):

        service_key = request.headers.get('IFTTT-Service-Key', '')
        channel_key = request.headers.get('IFTTT-Channel-Key', '')
        if service_key == SERVICE_KEY and channel_key == CHANNEL_KEY:
            # continue
            return f(*args, **kwargs)

        else:
            # IFTTT requests to return unauthorized(401)
            msg = "The server could not verify that you are authorized to access the URL requested." \
                  " You either supplied the wrong credentials (e.g. a bad password), or your browser" \
                  " doesn't understand how to supply the credentials required."
            errors = [{
                "message": msg
            }]

            body = json.dumps({'errors': errors}, ensure_ascii=False).decode('utf-8')
            resp = make_response(body, UNAUTHORIZED, {"Content-Type": "application/json; charset=utf-8"})
            return resp
            #return abort(401)

    return decorated_function
