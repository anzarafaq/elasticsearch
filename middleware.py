# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from logging import getLogger
from urlparse import parse_qs
import traceback

from simplejson import dumps
from werkzeug import Response as APIResponse
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import InternalServerError

logger = getLogger('milo_logger')


class ErrorMiddleware(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        try:
            return self.app(environ, start_response)
        except BadRequest, e:
            response = APIResponse(
                dumps(dict(error=e.description)),
                mimetype='application/json',
                status=e.code)
        except Exception, e:
            params = parse_qs(environ['QUERY_STRING'])
            error_msg = 'Internal error'
            trace = traceback.format_exc()
            if '_e' in params:
                error_msg = traceback.format_exc()
            error_msg = trace
            logger.debug(traceback.format_exc())
            #print traceback.format_exc()
            if type(e) == InternalServerError:
                logger.error(e.description)
            else:
                logger.exception('Unhandled exception!')
            response = APIResponse(
                dumps(dict(error=error_msg)),
                mimetype='application/json',
                status=500)
        return response(environ, start_response)
