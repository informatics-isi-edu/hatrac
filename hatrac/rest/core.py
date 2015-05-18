
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core service logic and dispatch rules for Hatrac REST API

"""

import re
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
import sys
import traceback
import hashlib

try:
    _webauthn2_config = webauthn2.merge_config(jsonFileName='webauthn2_config.json')
except:
    _webauthn2_config = webauthn2.merge_config()

_webauthn2_manager = webauthn2.Manager(overrides=_webauthn2_config)

def hash_list(l):
    copy = [ s.replace(';', ';;') for s in l ]
    copy.sort()
    return base64.b64encode(hashlib.md5(';'.join(copy)).digest())

def hash_dict(d):
    copy = [ (k, hash_list(v)) for k, v in d.items() ]
    copy.sort(key=lambda p: p[0])
    return ";".join([ v for k, v in copy ])

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

class NotModified (RestException):
    status = '304 Not Modified'
    message = 'Resource not modified.'

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

class PreconditionFailed (RestException):
    status = '412 Precondition Failed'
    message = 'Resource state does not match requested preconditions.'

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

            if hatrac.directory.prefix is None:
                # set once from web context if administrator did not specify in config
                hatrac.directory.prefix = web.ctx.env['SCRIPT_NAME']
            
            try:
                # get client authentication context
                web.ctx.webauthn2_context = _webauthn2_manager.get_request_context()
            except (ValueError, IndexError), ev:
                raise Unauthorized('service access requires client authentication')

            try:
                # run actual method
                for buf in original_method(*args):
                    yield buf
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
        self.get_body = True
        self.http_etag = None
        self.http_vary = _webauthn2_manager.get_http_vary()

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

        def etags_parse(s):
            etags = []
            s, strong = etag_parse(s)
            while s:
                s = s.strip()
                m = re.match('^,? *(?P<first>(W/)?"(.|\\")*")(?P<rest>.*)', s)
                if m:
                    g = m.groupdict()
                    etags.append(etag_parse(g['first']))
                    s = g['rest']
                    continue
                m = re.match('^,? *[*](?P<rest>.*)', s)
                if m:
                    g = m.groupdict()
                    etags.append((True, True))
                    s = g['rest']
                    continue
                s = None

            return dict(etags)
        
        return etags_parse(header)
        
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
             
            except (BadRange, NotImplemented), ev:
                raise ev
            except Exception, ev:
                # HTTP spec says to ignore a Range header w/ syntax errors
                web.debug('Ignoring HTTP Range header %s due to error: %s' % (get_range, ev))
                pass

        if get_slice is not None:
            nbytes, content_type, content_md5, data_generator \
                = resource.get_content_range(client_context, get_slice, get_data=self.get_body)
            web.ctx.status = '206 Partial Content'
            web.header(
                'Content-Range', 'bytes %d-%d/%d' 
                % (get_slice.start, get_slice.stop - 1, resource.nbytes)
            )
        else:
            nbytes, content_type, content_md5, data_generator = resource.get_content(client_context, get_data=self.get_body)
            web.ctx.status = '200 OK'

        if resource.is_object() and resource.is_version():
            web.header('Content-Disposition', "filename*=UTF-8''%s" % urllib.quote(str(resource.object).split("/")[-1]))
            
        web.header('Content-Length', nbytes)
        if content_type:
            web.header('Content-Type', content_type)
        if content_md5:
            web.header('Content-MD5', base64.b64encode(content_md5.strip()))
        if self.http_etag:
            web.header('ETag', self.http_etag)
        if self.http_vary:
            web.header('Vary', ', '.join(self.http_vary))

        if self.get_body:
            for buf in data_generator:
                yield buf

    def create_response(self, resource):
        """Form response for resource creation request."""
        web.ctx.status = '201 Created'
        web.header('Location', resource.asurl())
        web.header('Content-Type', 'text/uri-list')
        body = resource.asurl() + '\n'
        web.header('Content-Length', len(body))
        return body

    def delete_response(self):
        """Form response for deletion request."""
        web.ctx.status = '204 No Content'
        return ''

    def update_response(self):
        """Form response for update request."""
        web.ctx.status = '204 No Content'
        return ''

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
            return self._GET(*args)
        else:
            raise NoMethod('Method HEAD not supported for this resource.')

