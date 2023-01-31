
#
# Copyright 2015-2023 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core service logic and dispatch rules for Hatrac REST API

"""

import re
import logging
from logging.handlers import SysLogHandler
import json
from collections import OrderedDict
import random
import base64
import datetime
from datetime import timezone
import struct
import urllib
import sys
import traceback
import hashlib
from flask import request, g as hatrac_ctx
import flask.views
import werkzeug.exceptions
import werkzeug.http

import webauthn2
from webauthn2.util import Context, context_from_environment
from webauthn2.rest import format_trace_json, format_final_json

from . import app
from .. import core
from ..core import hatrac_debug, negotiated_content_type
from .. import directory

_webauthn2_manager = webauthn2.Manager()

def hash_value(d):
    return base64.b64encode(hashlib.md5(d.encode()).digest()).decode()

def hash_multi(d):
    if d is None:
        return '_'
    elif isinstance(d, (str, bytes)):
        return hash_value(d)
    elif isinstance(d, (list, set)):
        return hash_list(d)
    elif isinstance(d, dict):
        return hash_dict(d)
    else:
        raise NotImplementedError('hash %s' % type(d))

def hash_list(l):
    copy = [ hash_multi(s) for s in l ]
    copy.sort()
    return hash_value(''.join(copy))

def hash_dict(d):
    return hash_list([ hash_multi(k) + hash_multi(v) for k, v in d.items() ])

## setup logger and web request log helpers
logger = logging.getLogger('hatrac')
sysloghandler = SysLogHandler(address='/dev/log', facility=SysLogHandler.LOG_LOCAL1)
syslogformatter = logging.Formatter('%(name)s[%(process)d.%(thread)d]: %(message)s')
sysloghandler.setFormatter(syslogformatter)
logger.addHandler(sysloghandler)
logger.setLevel(logging.INFO)

def request_trace(tracedata):
    """Log one tracedata event as part of a request's audit trail.

       tracedata: a string representation of trace event data
    """
    logger.info(format_trace_json(
        tracedata,
        start_time=hatrac_ctx.hatrac_start_time,
        req=hatrac_ctx.hatrac_request_guid,
        client=request.remote_addr,
        webauthn2_context=hatrac_ctx.webauthn2_context,
    ))

class RestException (werkzeug.exceptions.HTTPException):
    """Hatrac generic REST exception overriding flask/werkzeug defaults.

    Our API defaults to text/plain error responses but supports
    negotiated HTML and some customization by legacy
    hatrac_config.json content.
    """

    # werkzeug fields
    code = None
    description = None

    # refactoring of prior hatrac templating
    title = None
    response_templates = OrderedDict([
        ("text/plain", "%(message)s"),
        ("text/html", "<html><body><h1>%(title)s</h1><p>%(message)s</p></body></html>"),
    ])

    def __init__(self, description=None, headers={}):
        self.headers = dict(headers)
        if description is not None:
            self.description = description
        super().__init__()
        # allow ourselves to customize the error title for our UX
        if self.title is None:
            self.title = werkzeug.http.HTTP_STATUS_CODES.get(self.code)

        # lookup templates overrides in hatrac_config.json
        #
        # OrderedDict.update() maintains ordering for keys already
        # controlled above, but has indeterminate order for new
        # additions from JSON dict!
        #
        # default templates override built-in templates
        self.response_templates = self.response_templates.copy()
        self.response_templates.update(
            core.config.get('error_templates', {}).get("default", {})
        )
        # code-specific templates override default templates
        self.response_templates.update(
            core.config.get('error_templates', {}).get(str(self.code), {})
        )
        # legacy config syntax
        #   code_typesuffix: template,
        #   ...
        for content_type in list(self.response_templates.keys()):
            template_key = '%s_%s' % (self.code, content_type.split('/')[-1])
            if template_key in core.config:
                self.response_templates[content_type] = core.config[template_key]

        # find client's negotiated type
        supported_content_types = list(self.response_templates.keys())
        default_content_type = supported_content_types[0]
        self.content_type = negotiated_content_type(request.environ, supported_content_types, default_content_type)
        self.headers['content-type'] = self.content_type

    # override the werkzeug base exception to use our state management
    def get_description(self, environ=None, scope=None):
        return self.description

    def get_body(self, environ=None, scope=None):
        template = self.response_templates[self.content_type]
        description = self.get_description()
        return (template + '\n') % {
            "code": self.code,
            "description": description,
            "message": description, # for existing hatrac_config template feature
            "title": self.title, # for our new generic templates
        }

    def get_headers(self, environ=None, scope=None):
        return self.headers

class NotModified (RestException):
    code = 304
    description = None

class BadRequest (RestException):
    code = 400
    description = 'Request malformed.'

class Unauthorized (RestException):
    code = 401
    description = 'Access requires authentication.'
    title = 'Authentication Required'

class Forbidden (RestException):
    code = 403
    description = 'Access forbidden.'
    title = 'Access Forbidden'

class NotFound (RestException):
    code = 404
    description = 'Resource not found.'

class NoMethod (RestException):
    code = 405
    description = 'Request method not allowed on this resource.'

class Conflict (RestException):
    code = 409
    description = 'Request conflicts with state of server.'

class LengthRequired (RestException):
    code = 411
    description = 'Content-Length header is required for this request.'

class PreconditionFailed (RestException):
    code = 412
    description = 'Resource state does not match requested preconditions.'

class PayloadTooLarge (RestException):
    code = 413
    description = 'Request body size is larger than the current limit defined by the server, which is %s bytes.' % \
              core.config.get("max_request_payload_size", core.max_request_payload_size_default)

class BadRange (RestException):
    code = 416
    description = 'Requested Range is not satisfiable for this resource.'

    def __init__(self, description=None, headers={}, nbytes=None):
        super().__init__(description=description, headers=headers)
        if nbytes is not None:
            self.headers['content-range'] = 'bytes */%d' % nbytes

class NotImplemented (RestException):
    code = 501
    description = 'Request not implemented for this resource.'

class ServerError (RestException):
    code = 500
    description = 'The request encountered an error on the server.'

@app.before_request
def before_request():
    # request context init
    hatrac_ctx.hatrac_status = None
    hatrac_ctx.hatrac_request_guid = base64.b64encode( struct.pack('Q', random.getrandbits(64)) ).decode()
    hatrac_ctx.hatrac_start_time = datetime.datetime.now(timezone.utc)
    hatrac_ctx.hatrac_request_content_range = None
    hatrac_ctx.hatrac_content_type = None
    hatrac_ctx.webauthn2_manager = _webauthn2_manager
    hatrac_ctx.webauthn2_context = webauthn2.Context() # set empty context for sanity
    hatrac_ctx.hatrac_request_trace = request_trace
    hatrac_ctx.hatrac_directory = directory

    if directory.prefix is None:
        # set once from web context if administrator did not specify in config
        directory.prefix = request.environ['SCRIPT_NAME']

    # get client authentication context
    hatrac_ctx.webauthn2_context = context_from_environment(request.environ, fallback=True)

    return None

@app.after_request
def after_request(response):
    if isinstance(response, flask.Response):
        hatrac_ctx.hatrac_status = response.status
    elif isinstance(response, RestException):
        hatrac_ctx.hatrac_status = response.code
    hatrac_ctx.hatrac_content_type = response.headers.get('content-type', 'none')
    if 'content-range' in response.headers:
        content_range = response.headers['content-range']
        if content_range.startswith('bytes '):
            content_range = content_range[6:]
        hatrac_ctx.hatrac_request_content_range = content_range
    elif 'content-length' in response.headers:
        hatrac_ctx.hatrac_request_content_range = '*/%s' % response.headers['content-length']
    else:
        hatrac_ctx.hatrac_request_content_range = '*/0'
    logger.info(format_final_json(
        environ=request.environ,
        webauthn2_context=hatrac_ctx.webauthn2_context,
        req=hatrac_ctx.hatrac_request_guid,
        start_time=hatrac_ctx.hatrac_start_time,
        client=request.remote_addr,
        status=hatrac_ctx.hatrac_status,
        content_range=hatrac_ctx.hatrac_request_content_range,
        content_type=hatrac_ctx.hatrac_content_type,
        track=(hatrac_ctx.webauthn2_context.tracking if hatrac_ctx.webauthn2_context else None),
    ))
    return response

@app.errorhandler(Exception)
def error_handler(ev):
    if isinstance(ev, core.HatracException):
        # map these core errors to RestExceptions
        ev = {
            core.BadRequest: BadRequest,
            core.Conflict: Conflict,
            core.Forbidden: Forbidden,
            core.Unauthenticated: Unauthorized,
            core.NotFound: NotFound,
        }[type(ev)](str(ev))

    if isinstance(ev, (RestException, werkzeug.exceptions.HTTPException)):
        # trace unless not really an error
        if isinstance(ev, NotModified):
            pass
        else:
            request_trace(str(ev))
    else:
        # log other internal server errors
        et, ev2, tb = sys.exc_info()
        hatrac_debug(
            'Got unhandled exception in hatrac request handler: %s\n%s\n' % (
                ev,
                traceback.format_exception(et, ev2, tb),
            )
        )
        ev = ServerError(str(ev))

    return ev

class RestHandler (flask.views.MethodView):
    """Generic implementation logic for Hatrac REST API handlers.

    """
    def __init__(self):
        self.get_body = True
        self.http_etag = None
        self.http_vary = _webauthn2_manager.get_http_vary()

    def trace(self, msg):
        hatrac_ctx.hatrac_request_trace(msg)
        
    def _fullname(self, path, name):
        nameparts = [ n for n in path.split('/') if n ]
        if name:
            nameparts.append(name)
        fullname = '/' + '/'.join(nameparts)
        return fullname

    def resolve(self, path, name, raise_notfound=True):
        fullname = self._fullname(path, name)
        return hatrac_ctx.hatrac_directory.name_resolve(fullname, raise_notfound)

    def resolve_version(self, path, name, version):
        object = self.resolve(path, name)
        return object.version_resolve(version)

    def resolve_upload(self, path, name, job):
        resource = self.resolve(path, name)
        return resource.upload_resolve(job)
        
    def resolve_name_or_version(self, path, name, version):
        if version:
            return self.resolve_version(path, name, version)
        else:
            return self.resolve(path, name)

    def in_content_type(self):
        in_content_type = request.headers.get('content-type')
        if in_content_type is not None:
            return in_content_type.lower().split(";", 1)[0].strip()
        else:
            return None

    def set_http_etag(self, version):
        """Set an ETag from version key.

        """
        etag = []
        etag.append( '%s' % version )

        self.http_etag = '"%s"' % ';'.join(etag).replace('"', '\\"')

    def parse_client_etags(self, header):
        """Parse header string for ETag-related preconditions.

           Returns dict mapping ETag -> boolean indicating strong
           (true) or weak (false).

           The special key True means the '*' precondition was
           encountered which matches any representation.

        """
        def etag_parse(s):
            strong = True
            if s[0:2] == 'W/':
                strong = False
                s = s[2:]
            return (s, strong)

        s = header
        etags = []
        # pick off one ETag prefix at a time, consuming comma-separated list
        while s:
            s = s.strip()
            # accept leading comma that isn't really valid by spec...
            m = re.match('^,? *(?P<first>(W/)?"([^"]|\\\\")*")(?P<rest>.*)', s)
            if m:
                # found 'W/"tag"' or '"tag"'
                g = m.groupdict()
                etags.append(etag_parse(g['first']))
                s = g['rest']
                continue
            m = re.match('^,? *[*](?P<rest>.*)', s)
            if m:
                # found '*'
                # accept anywhere in list even though spec is more strict...
                g = m.groupdict()
                etags.append((True, True))
                s = g['rest']
                continue
            s = None

        return dict(etags)
        
    def http_check_preconditions(self, method='GET', resource_exists=True):
        failed = False

        match_etags = self.parse_client_etags(request.environ.get('HTTP_IF_MATCH', ''))
        if match_etags:
            if resource_exists:
                if self.http_etag and self.http_etag not in match_etags \
                   and (True not in match_etags):
                    failed = True
            else:
                failed = True
        
        nomatch_etags = self.parse_client_etags(request.environ.get('HTTP_IF_NONE_MATCH', ''))
        if nomatch_etags:
            if resource_exists:
                if self.http_etag and self.http_etag in nomatch_etags \
                   or (True in nomatch_etags):
                    failed = True

        if failed:
            headers={ 
                "ETag": self.http_etag, 
                "Vary": ", ".join(self.http_vary)
            }
            if method == 'GET':
                raise NotModified(headers=headers)
            else:
                raise PreconditionFailed(headers=headers)

    def get_content(self, resource, client_context, get_body=True):
        """Form response w/ bulk resource content."""
        get_range = request.environ.get('HTTP_RANGE')
        get_slice = None
        if get_range:
            # parse HTTP Range header which can encode a set of ranges
            get_slices = []

            if resource.is_object() and not resource.is_version():
                # lookup version so we can get at nbytes
                resource = resource.get_current_version()

            if not hasattr(resource, 'nbytes') \
               or not hasattr(resource, 'get_content_range'):
                raise NotImplemented('Range requests not implemented for resource %s.' % resource)

            try:
                units, rset = get_range.split('=')

                if units.lower() != "bytes":
                    raise NotImplemented('Range requests with units "%s" not implemented.' % units)

                for r in rset.split(","):
                    first, last = r.split("-", 1)
                    if first == '':
                        # a suffix request
                        length = int(last)
                        if length == 0:
                            # zero length suffix is syntactically invalid?
                            raise ValueError('zero length suffix')
                            
                        if length > resource.nbytes:
                            length = resource.nbytes

                        get_slices.append(
                            slice(
                                max(0, resource.nbytes - length),
                                resource.nbytes
                            )
                        )
                    else:
                        first = int(first)
                        if last == '':
                            # an open [first, eof] request
                            get_slices.append(
                                slice(first, resource.nbytes)
                            )
                        else:
                            # a closed [first, last] request
                            last = int(last)
                            if last < first:
                                raise ValueError('last < first')
                            get_slices.append(
                                slice(first, last+1)
                            )

                def intersects_resource(r):
                    if r.start < resource.nbytes and r.start < (r.stop + 1):
                        return True
                    else:
                        return False

                get_slices = [ slc for slc in get_slices if intersects_resource(slc) ]
                
                if len(get_slices) == 0:
                    raise BadRange(
                        'Range not satisfiable for resource %s.' % resource, 
                        nbytes=resource.nbytes
                    )
                elif len(get_slices) > 1:
                    raise NotImplemented(
                        'Multiple range request not implemented for resource %s.' % resource
                    )
                else:
                    get_slice = get_slices[0]
                    get_slice = slice(get_slice.start, min(get_slice.stop, resource.nbytes))
                    if get_slice.start == 0 and get_slice.stop == resource.nbytes:
                        # whole-entity
                        get_slice = None
             
            except (BadRange, NotImplemented) as ev:
                raise ev
            except Exception as ev:
                # HTTP spec says to ignore a Range header w/ syntax errors
                hatrac_debug('Ignoring HTTP Range header %s due to error: %s' % (get_range, ev))
                pass

        status = 200
        headers = {}

        if get_slice is not None:
            nbytes, metadata, data_generator \
                = resource.get_content_range(client_context, get_slice, get_data=self.get_body)
            status = 206
            crange = '%d-%d/%d' % (get_slice.start, get_slice.stop - 1, resource.nbytes)
            headers['content-range'] = 'bytes %s' % crange
        else:
            nbytes, metadata, data_generator = resource.get_content(client_context, get_data=self.get_body)

        if resource.is_object() and self.get_body is False:
            headers['accept-ranges'] = 'bytes'
        headers['Content-Length'] = nbytes

        metadata = core.Metadata(metadata)
        metadata = metadata.to_http()
        headers.update(metadata)

        if resource.is_object() and resource.is_version():
            headers['content-location'] = resource.asurl()
            if 'content-disposition' not in resource.metadata:
                headers['content-disposition'] = "filename*=UTF-8''%s" % urllib.parse.quote(str(resource.object).split("/")[-1])
            
        if self.http_etag:
            headers['ETag'] = self.http_etag
            
        if self.http_vary:
            headers['Vary'] = ', '.join(self.http_vary)

        return (data_generator, status, headers)

    def create_response(self, resource):
        """Form response for resource creation request."""
        status = 201
        content_type = 'text/uri-list'
        body = resource.asurl() + '\n'
        nbytes = len(body)
        headers = {
            'location': resource.asurl(),
            'content-type': content_type,
            'content-length': nbytes,
        }
        return (body, status, headers)

    def delete_response(self):
        """Form response for deletion request."""
        return ('', 204)

    def update_response(self):
        """Form response for update request."""
        return ('', 204)

    def redirect_response(self, redirect):
        """Form response for redirect."""
        assert isinstance(redirect, core.Redirect)
        status = 303
        content_type = 'text/uri-list'
        body = redirect.url + '\n'
        nbytes = len(body)
        headers = {
            'location': redirect.url,
            'content-type': content_type,
            'content-length': nbytes,
        }
        return (body, status, headers)
