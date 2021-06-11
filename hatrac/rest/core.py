
#
# Copyright 2015-2019 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core service logic and dispatch rules for Hatrac REST API

"""

import re
import logging
from logging.handlers import SysLogHandler
import web
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

import webauthn2
from webauthn2.util import context_from_environment
from webauthn2.rest import get_log_parts, request_trace_json, request_final_json

from .. import core
from .. import directory

_webauthn2_manager = webauthn2.Manager()


def hash_value(d):
    return base64.b64encode(hashlib.md5(d.encode()).digest()).decode()

def hash_multi(d):
    if d is None:
        return '_'
    elif isinstance(d, str):
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

def log_parts():
    """Generate a dictionary of interpolation keys used by our logging template."""
    return get_log_parts('hatrac_start_time', 'hatrac_request_guid', 'hatrac_request_content_range', 'hatrac_content_type')

def request_trace(tracedata):
    """Log one tracedata event as part of a request's audit trail.

       tracedata: a string representation of trace event data
    """
    logger.info( request_trace_json(tracedata, log_parts()) )

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
        web.HTTPError.__init__(self, self.status, hdr, msg + '\n' if msg is not None else '')
        web.ctx.hatrac_content_type = hdr['Content-Type']

class NotModified (RestException):
    status = '304 Not Modified'
    message = None

class BadRequest (RestException):
    status = '400 Bad Request'
    message = 'Request malformed.'

class TemplatedRestException (RestException):
    error_type = ''
    supported_content_types = ['text/plain', 'text/html']
    def __init__(self, message=None, headers=None):
        # filter types to those for which we have a response template, or text/plain which we always support
        supported_content_types = [
            content_type for content_type in self.supported_content_types
            if "%s_%s" % (self.error_type, content_type.split('/')[-1]) in core.config or content_type == 'text/plain'
        ]
        default_content_type = supported_content_types[0]
        # find client's preferred type
        content_type = webauthn2.util.negotiated_content_type(supported_content_types, default_content_type)
        # lookup template and use it if available
        template_key = '%s_%s' % (self.error_type, content_type.split('/')[-1])
        if template_key in core.config:
            message = core.config[template_key] % dict(message=message)
        RestException.__init__(self, message, headers)
        web.header('Content-Type', content_type)
        
class Unauthorized (TemplatedRestException):
    error_type = '401'
    status = '401 Unauthorized'
    message = 'Access requires authentication.'

class Forbidden (TemplatedRestException):
    error_type = '403'
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

class PreconditionFailed (RestException):
    status = '412 Precondition Failed'
    message = 'Resource state does not match requested preconditions.'

class PayloadTooLarge (RestException):
    status = '413 Payload Too Large'
    message = 'Request body size is larger than the current limit defined by the server, which is %s bytes.' % \
              core.config.get("max_request_payload_size", core.max_request_payload_size_default)

class BadRange (RestException):
    status = '416 Requested Range Not Satisfiable'
    message = 'Requested Range is not satisfiable for this resource.'
    def __init__(self, msg=None, headers=None, nbytes=None):
        RestException.__init__(self, msg, headers)
        if nbytes is not None:
            web.header('Content-Range', 'bytes */%d' % nbytes)

class NotImplemented (RestException):
    status = '501 Not Implemented'
    message = 'Request not implemented for this resource.'

class ServerError (RestException):
    status = '500 Internal Server Error'
    message = 'The request encountered an error on the server: %s.'

def web_method():
    """Augment web handler method with common service logic."""
    def helper(original_method):
        def wrapper(*args):
            # request context init
            web.ctx.hatrac_request_guid = base64.b64encode( struct.pack('Q', random.getrandbits(64)) ).decode()
            web.ctx.hatrac_start_time = datetime.datetime.now(timezone.utc)
            web.ctx.hatrac_request_content_range = None
            web.ctx.hatrac_content_type = None
            web.ctx.webauthn2_manager = _webauthn2_manager
            web.ctx.webauthn2_context = webauthn2.Context() # set empty context for sanity
            web.ctx.hatrac_request_trace = request_trace
            web.ctx.hatrac_directory = directory

            if directory.prefix is None:
                # set once from web context if administrator did not specify in config
                directory.prefix = web.ctx.env['SCRIPT_NAME']
            
            try:
                # get client authentication context
                web.ctx.webauthn2_context = context_from_environment(fallback=False)
                if web.ctx.webauthn2_context is None:
                    web.debug("falling back to _webauthn2_manager.get_request_context() after failed context_from_environment(False)")
                    web.ctx.webauthn2_context = _webauthn2_manager.get_request_context()
            except (ValueError, IndexError) as ev:
                raise Unauthorized('service access requires client authentication')

            try:
                # run actual method
                return original_method(*args)

            except core.BadRequest as ev:
                request_trace(str(ev))
                raise BadRequest(str(ev))
            except core.Unauthenticated as ev:
                request_trace(str(ev))
                raise Unauthorized(str(ev))
            except core.Forbidden as ev:
                request_trace(str(ev))
                raise Forbidden(str(ev))
            except core.NotFound as ev:
                request_trace(str(ev))
                raise NotFound(str(ev))
            except core.Conflict as ev:
                request_trace(str(ev))
                raise Conflict(str(ev))
            except RestException as ev:
                # pass through rest exceptions already generated by handlers
                if not isinstance(ev, NotModified):
                    request_trace(str(ev))
                raise
            except Exception as ev:
                # log and rethrow all errors so web.ctx reflects error prior to request_final_log below...
                et, ev2, tb = sys.exc_info()
                web.debug(
                    'Got unhandled exception in web_method()',
                    ev,
                    traceback.format_exception(et, ev2, tb),
                )
                raise ServerError(str(ev))
            finally:
                # finalize
                logger.info( request_final_json(log_parts()) )
        return wrapper
    return helper
                
class RestHandler (object):
    """Generic implementation logic for Hatrac REST API handlers.

    """
    def __init__(self):
        self.get_body = True
        self.http_etag = None
        self.http_vary = _webauthn2_manager.get_http_vary()

    def trace(self, msg):
        web.ctx.hatrac_request_trace(msg)
        
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

    def resolve_upload(self, path, name, job):
        resource = self.resolve(path, name)
        return resource.upload_resolve(job)
        
    def resolve_name_or_version(self, path, name, version):
        if version:
            return self.resolve_version(path, name, version)
        else:
            return self.resolve(path, name)

    def in_content_type(self):
        in_content_type = web.ctx.env.get('CONTENT_TYPE')
        if in_content_type is not None:
            return in_content_type.lower().split(";", 1)[0].strip()
        else:
            return None

    def parse_querystr(self, querystr):
        params = querystr.split('&')
        result = {}
        for param in params:
            if param:
                parts = param.split('=')
                if parts:
                    result[ parts[0] ] = '='.join(parts[1:])
        return result
        
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

        match_etags = self.parse_client_etags(web.ctx.env.get('HTTP_IF_MATCH', ''))
        if match_etags:
            if resource_exists:
                if self.http_etag and self.http_etag not in match_etags \
                   and (True not in match_etags):
                    failed = True
            else:
                failed = True
        
        nomatch_etags = self.parse_client_etags(web.ctx.env.get('HTTP_IF_NONE_MATCH', ''))
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
        get_range = web.ctx.env.get('HTTP_RANGE')
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
                web.debug('Ignoring HTTP Range header %s due to error: %s' % (get_range, ev))
                pass

        if get_slice is not None:
            nbytes, metadata, data_generator \
                = resource.get_content_range(client_context, get_slice, get_data=self.get_body)
            web.header(
                'Content-Range', 'bytes %d-%d/%d' 
                % (get_slice.start, get_slice.stop - 1, resource.nbytes)
            )
            web.ctx.hatrac_request_content_range = '%d-%d/%d' % (get_slice.start, get_slice.stop - 1, resource.nbytes)
            web.ctx.status = '206 Partial Content'
        else:
            nbytes, metadata, data_generator = resource.get_content(client_context, get_data=self.get_body)
            web.ctx.hatrac_request_content_range = '*/%d' % nbytes
            web.ctx.status = '200 OK'

        web.header('Content-Length', nbytes)

        metadata = core.Metadata(metadata)
        if 'content-type' in metadata:
            web.ctx.hatrac_content_type = metadata['content-type']

        metadata = metadata.to_http()
            
        for hdr, val in metadata.items():
            web.header(hdr, val)

        if resource.is_object() and resource.is_version():
            web.header('Content-Location', resource.asurl())
            if 'content-disposition' not in resource.metadata:
                web.header('Content-Disposition', "filename*=UTF-8''%s" % urllib.parse.quote(str(resource.object).split("/")[-1]))
            
        if self.http_etag:
            web.header('ETag', self.http_etag)
            
        if self.http_vary:
            web.header('Vary', ', '.join(self.http_vary))

        return data_generator

    def create_response(self, resource):
        """Form response for resource creation request."""
        web.ctx.status = '201 Created'
        web.header('Location', resource.asurl())
        content_type = 'text/uri-list'
        web.header('Content-Type', content_type)
        web.ctx.hatrac_content_type = content_type
        body = resource.asurl() + '\n'
        nbytes = len(body)
        web.header('Content-Length', nbytes)
        web.ctx.hatrac_request_content_range = '*/%d' % nbytes
        return body

    def delete_response(self):
        """Form response for deletion request."""
        web.ctx.status = '204 No Content'
        web.ctx.hatrac_request_content_range = '*/0'
        web.ctx.hatrac_content_type = 'none'
        return ''

    def update_response(self):
        """Form response for update request."""
        web.ctx.status = '204 No Content'
        web.ctx.hatrac_request_content_range = '*/0'
        web.ctx.hatrac_content_type = 'none'
        return ''

    def redirect_response(self, redirect):
        """Form response for redirect."""
        assert isinstance(redirect, core.Redirect)
        web.header('Location', redirect.url)
        web.ctx.status = '303 See Other'
        content_type = 'text/uri-list'
        web.header('Content-Type', content_type)
        web.ctx.hatrac_content_type = content_type
        body = redirect.url + '\n'
        nbytes = len(body)
        web.header('Content-Length', nbytes)
        web.ctx.hatrac_request_content_range = '*/%d' % nbytes
        return body


    @web_method()
    def GET(self, *args):
        """Get resource."""
        if hasattr(self, '_GET'):
            return self._GET(*args)
        else:
            raise NoMethod('Method GET not supported for this resource.')

    @web_method()
    def HEAD(self, *args):
        """Get resource metadata."""
        self.get_body = False
        if hasattr(self, '_GET'):
            result = self._GET(*args)
            web.ctx.hatrac_request_content_range = '*/0'
            web.ctx.hatrac_content_type = 'none'
            return result
        else:
            raise NoMethod('Method HEAD not supported for this resource.')

