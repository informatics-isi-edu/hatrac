
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core service logic and dispatch rules for Hatrac REST API

"""

import logging
from logging.handlers import SysLogHandler
import web
import random
import base64
import datetime
import pytz
import webauthn2
import struct
import urllib
import hatrac
import hatrac.core

_webauthn2_config = webauthn2.merge_config(
    jsonFileName='webauthn2_config.json'
)
# TODO: coordinate web_cookie_path setting of webauthn2?
_webauthn2_manager = webauthn2.Manager(overrides=_webauthn2_config)


# map URL pattern (regexp) to handler class
dispatch_rules = dict()

def web_url(url_patterns):
    """Annotate and track web request handler class and dispatch URL patterns.

       url_patterns: sequence of URL regular expression patterns that
         will be mapped to the modified handler class in web.py
         dispatch

    """
    def helper(original_class):
        original_class.url_patterns = url_patterns
        for url_pattern in url_patterns:
            assert url_pattern not in dispatch_rules
            dispatch_rules[url_pattern] = original_class
        return original_class
    return helper

## setup logger and web request log helpers
logger = logging.getLogger('hatrac')
sysloghandler = SysLogHandler(address='/dev/log', facility=SysLogHandler.LOG_LOCAL1)
syslogformatter = logging.Formatter('%(name)s[%(process)d.%(thread)d]: %(message)s')
sysloghandler.setFormatter(syslogformatter)
logger.addHandler(sysloghandler)
logger.setLevel(logging.INFO)

# some log message templates
log_template = "%(elapsed_s)d.%(elapsed_ms)3.3ds %(client_ip)s user=%(client_identity)s req=%(reqid)s"
log_trace_template = log_template + " -- %(tracedata)s"
log_final_template = log_template + " (%(status)s) %(method)s %(proto)s://%(host)s/%(uri)s %(range)s %(type)s"

def log_parts():
    """Generate a dictionary of interpolation keys used by our logging template."""
    now = datetime.datetime.now(pytz.timezone('UTC'))
    elapsed = (now - web.ctx.hatrac_start_time)
    parts = dict(
        elapsed_s = elapsed.seconds, 
        elapsed_ms = elapsed.microseconds/1000,
        client_ip = web.ctx.ip,
        client_identity = web.ctx.webauthn2_context and urllib.quote(web.ctx.webauthn2_context.client or '') or '',
        reqid = web.ctx.hatrac_request_guid
        )
    return parts
    
def request_trace(tracedata):
    """Log one tracedata event as part of a request's audit trail.

       tracedata: a string representation of trace event data
    """
    parts = log_parts()
    parts['tracedata'] = tracedata
    logger.info( (log_trace_template % parts).encode('utf-8') )

class RestException (web.HTTPError):
    message = None
    status = None
    headers = {
        'Content-Type': 'text/plain'
    }

    def __init__(self, message=None, headers=None):
        if headers:
            hdr = dict(self.headers)
            hdr.update(headers)
        else:
            hdr = self.headers
        msg = message or self.message
        web.HTTPError.__init__(self, self.status, hdr, msg + '\n')

class BadRequest (RestException):
    status = '400 Bad Request'
    message = 'Request malformed.'

class Unauthorized (RestException):
    status = '401 Unauthorized'
    message = 'Access requires authentication.'

class Forbidden (RestException):
    status = '403 Forbidden'
    message = 'Access forbidden.'

class NotFound (RestException):
    status = '404 Not Found'
    message = 'Resource not found.'

class NoMethod (RestException):
    status = '405 Method Not Allowed'
    message = 'Request method not allowed on this resource.'

class Conflict (RestException):
    status = '409 Conflict'
    message = 'Request conflicts with state of server.'

class LengthRequired (RestException):
    status = '411 Length Required'
    message = 'Content-Length header is required for this request.'



def web_method():
    """Augment web handler method with common service logic."""
    def helper(original_method):
        def wrapper(*args):
            # request context init
            web.ctx.hatrac_request_guid = base64.b64encode( struct.pack('Q', random.getrandbits(64)) )
            web.ctx.hatrac_start_time = datetime.datetime.now(pytz.timezone('UTC'))
            web.ctx.hatrac_request_content_range = '-/-'
            web.ctx.hatrac_content_type = 'unknown'
            web.ctx.webauthn2_manager = _webauthn2_manager
            web.ctx.webauthn2_context = webauthn2.Context() # set empty context for sanity
            web.ctx.hatrac_request_trace = request_trace
            web.ctx.hatrac_directory = hatrac.directory

            try:
                # get client authentication context
                web.ctx.webauthn2_context = _webauthn2_manager.get_request_context()
            except (ValueError, IndexError):
                raise Unauthorized('service access')

            try:
                # run actual method
                return original_method(*args)
            except hatrac.core.BadRequest, ev:
                raise BadRequest(str(ev))
            except hatrac.core.Unauthenticated, ev:
                raise Unauthorized(str(ev))
            except hatrac.core.Forbidden, ev:
                raise Forbidden(str(ev))
            except hatrac.core.NotFound, ev:
                raise NotFound(str(ev))
            except hatrac.core.Conflict, ev:
                raise Conflict(str(ev))
            finally:
                # finalize
                parts = log_parts()
                parts.update(dict(
                    status = web.ctx.status,
                    method = web.ctx.method,
                    proto = web.ctx.protocol,
                    host = web.ctx.host,
                    uri = web.ctx.env['REQUEST_URI'],
                    range = web.ctx.hatrac_request_content_range,
                    type = web.ctx.hatrac_content_type
                ))
                logger.info( (log_final_template % parts).encode('utf-8') )
        return wrapper
    return helper
                
class RestHandler (object):
    """Generic implementation logic for Hatrac REST API handlers.

    """
    def __init__(self):
        pass

    def _fullname(self, path, name):
        nameparts = [ n for n in ((path or '') + (name or '')).split('/') if n ]
        fullname = '/' + '/'.join(nameparts)
        return fullname

    def resolve(self, path, name, raise_notfound=True):
        fullname = self._fullname(path, name)
        return web.ctx.hatrac_directory.name_resolve(fullname, raise_notfound)

    def resolve_version(self, path, name, version):
        object = self.resolve(path, name)
        return object.version_resolve(version)

    def resolve_name_or_version(self, path, name, version):
        if version:
            return self.resolve_version(path, name, version)
        else:
            return self.resolve(path, name)

    def in_content_type(self):
        in_content_type = web.ctx.env.get('CONTENT_TYPE').lower()
        if in_content_type is not None:
            return in_content_type.split(";", 1)[0].strip()
        else:
            return None

    def get_content(self, resource, client_context):
        """Form response w/ bulk resource content."""
        nbytes, data_generator = resource.get_content(client_context)
        web.ctx.status = '200 OK'
        # TODO: refactor headers into resource.get_content() result tuple
        web.header('Content-Length', nbytes)
        if resource.is_object() and resource.is_version():
            if resource.content_type:
                web.header('Content-Type', resource.content_type)
            if resource.content_md5:
                web.header('Content-MD5', resource.content_md5)
        else:
            web.header('Content-Type', 'application/json')
        for buf in data_generator:
            yield buf

    def create_response(self, resource):
        """Form response for resource creation request."""
        web.ctx.status = '201 Created'
        web.header('Location', str(resource))
        return ''

    def delete_response(self):
        """Form response for deletion request."""
        web.ctx.status = '204 No Content'
        return ''

    def update_response(self):
        """Form response for update request."""
        web.ctx.status = '204 No Content'
        return ''
