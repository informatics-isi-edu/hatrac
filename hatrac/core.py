
#
# Copyright 2015-2023 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core module config

"""

import sys
import binascii
import base64
import re
import urllib
import json

from webauthn2.util import merge_config, jsonWriter

config = merge_config(
    jsonFileName='hatrac_config.json',
    built_ins={
        # backwards compatible default firewall behavior
        "firewall_acls": {
            "create": ["*"],
            "delete": ["*"],
            "manage_acl": ["*"],
            "manage_metadata": ["*"],
        },
    },
)
# digest firewall acls into sets once for reuse across requests...
config["firewall_acls"] = { k: set(v) for k, v in config['firewall_acls'].items() }

max_request_payload_size_default = 1024 * 1024 * 128  # ~135MB

def set_acl_match_attributes(client_context):
    """Idempotently set client_context.acl_match_attributes"""
    if hasattr(client_context, 'acl_match_attributes'):
        return

    match_attributes = set([
        attr['id'] if isinstance(attr, dict) else attr
        for attr in client_context.attributes
    ])
    match_attributes.add('*')
    if client_context.client:
        client = client_context.client
        match_attributes.add(client['id'] if isinstance(client, dict) else client)
    client_context.acl_match_attributes = match_attributes

def hatrac_debug(*args):
    """Shim for non-logger diagnostics

    This stderr output will typically go to the web container's debug
    log.  We previously used web.py's web.debug function for this.
    """
    if len(args) > 1:
        v = str(tuple(args))
    else:
        v = str(args[0])

    print(v, file=sys.stderr, flush=True)

class web_storage(object):
    """Shim to emulate web.storage attr-dict class.

    This is used in legacy code before migrating from web.py to flask.
    """
    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)

    def __getattribute__(self, a):
        """Allow reading of dict keys as attributes.

        Don't allow dict keys to shadow actual attributes of dict, and
        proxy those instead.
        """
        sself = super(web_storage, self)
        try:
            return sself.__getattribute__(a)
        except:
            d = sself.__getattribute__('_d')
            if a in d:
                return d[a]
            else:
                raise AttributeError(a)

def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg

def _string_wrap(s, escape=u'\\', protect=[]):
    try:
        s = s.replace(escape, escape + escape)
        for c in set(protect):
            s = s.replace(c, escape + c)
        return s
    except Exception as e:
        #hatrac_debug('_string_wrap', s, escape, protect, e)
        raise

def sql_identifier(s):
    # double " to protect from SQL
    return u'"%s"' % _string_wrap(s, u'"')

def sql_literal(v):
    if type(v) is list:
        return 'ARRAY[%s]' % (','.join(map(sql_literal, v)))
    elif v is not None:
        # double ' to protect from SQL
        s = '%s' % v
        return "'%s'" % _string_wrap(s, u"'")
    else:
        return 'NULL'

def negotiated_content_type(environ, supported_types=['text/csv', 'application/json', 'application/x-json-stream'], default=None):
    """Determine negotiated response content-type from Accept header.

       environ: the WSGI environment containing HTTP_* header content

       supported_types: a list of MIME types the caller would be able
         to implement if the client has requested one.

       default: a MIME type or None to return if none of the
         supported_types were requested by the client.

       This function considers the preference qfactors encoded in the
       client request to choose the preferred type when there is more
       than one supported type that the client would accept.

    """
    def accept_pair(s):
        """parse one Accept header pair into (qfactor, type)."""
        parts = s.split(';')
        q = 1.0
        t = parts[0].strip()
        for p in parts[1:]:
            fields = p.split('=')
            if len(fields) == 2 and fields[0] == 'q':
                q = float(fields[1])
        return (q, t)

    try:
        accept = environ['HTTP_ACCEPT']
    except:
        accept = ""

    accept_types = [
        pair[1]
        for pair in sorted(
            [ accept_pair(s) for s in accept.lower().split(',') ],
            key=lambda pair: pair[0]
            )
        ]

    if accept_types:
        for accept_type in accept_types:
            if accept_type in supported_types:
                return accept_type

    return default

class ObjectVersionMissing(Exception):
    """Internal exception useful in storage backends.

    This would be an internal server error if raised from a storage
    backend, because it means the DB and storage are out of sync. But,
    the overlay provider may catch this and search additional
    backends...
    """
    pass

class HatracException (Exception):
    """Base class for Hatrac API exceptions."""
    pass

class BadRequest (HatracException):
    """Exceptions representing malformed requests."""
    pass

class Conflict (HatracException):
    """Exceptions representing conflict between usage and current state."""
    pass

class Forbidden (HatracException):
    """Exceptions representing lack of authorization for known client."""
    pass

class Unauthenticated (HatracException):
    """Exceptions representing lack of authorization for anonymous client."""
    pass

class NotFound (HatracException):
    """Exceptions representing attempted access to unknown resource."""
    pass

def _make_bin_decoder(nbytes, context=''):
    def helper(orig):
        if len(orig) == nbytes * 2:
            try:
                data = binascii.unhexlify(orig)
            except Exception as e:
                raise BadRequest('Could not hex-decode "%s". %s' % (orig, e))
        else:
            try:
                data = base64.b64decode(orig)
            except Exception as e:
                raise BadRequest('Could not base64 decode "%s". %s' % (orig, e))
                
        if len(data) != nbytes:
            raise BadRequest(
                'Could not decode "%s"%s into %d bytes using hex nor base64.' % (
                    orig,
                    context,
                    nbytes
                )
            )
        return data
    return helper
        
def _test_content_disposition(orig):
    m = re.match("^filename[*]=UTF-8''(?P<name>[-_.~A-Za-z0-9%]+)$", orig)
    if not m:
        raise BadRequest(
            'Cannot accept content-disposition "%s".' % orig
        )
    
    n = m.groupdict()['name']
    
    try:
        n = urllib.parse.unquote(n)
    except Exception as e:
        raise BadRequest(
            'Invalid URL encoding of content-disposition filename component. %s.' % e
        )
    
    if n.find("/") >= 0 or n.find("\\") >= 0:
        raise BadRequest(
            'Invalid occurrence of path divider in content-disposition filename component "%s" after URL and UTF-8 decoding.' % n
        )
    
    return orig

class MetadataValue (str):
    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.container.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = self + '\n'
        return len(body), Metadata({'content-type': 'text/plain'}), body

class MetadataBytes (bytes):
    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.container.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = base64.b64encode(self) + b'\n'
        return len(body), Metadata({'content-type': 'text/plain'}), body

    def encode(self):
        return self

class Metadata (dict):

    _all_keys = {
        'content-type',
        'content-disposition',
        'content-md5',
        'content-sha256'
    }

    _write_once_keys = {
        'content-md5',
        'content-sha256'
    }

    # { key: (coder, decoder)... }
    _sql_codecs = {
        'content-md5': (
            lambda s: binascii.hexlify(s.encode()).decode(),
            binascii.unhexlify
        ),
        'content-sha256': (
            lambda s: binascii.hexlify(s.encode()).decode(),
            binascii.unhexlify
        )
    }

    # { key: (coder, decoder)... }
    _http_codecs = {
        'content-disposition': (
            lambda x: x,
            _test_content_disposition
        ),
        'content-md5': (
            lambda s: base64.b64encode(s).decode(),
            _make_bin_decoder(16, ' for content-md5')
        ),
        'content-sha256': (
            lambda s: base64.b64encode(s).decode(),
            _make_bin_decoder(32, ' for content-sha256')
        )
    }

    def __init__(self, src={}):
        dict.__init__(self)
        for k, v in src.items():
            self[k] = v

    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = jsonWriter(self.to_http()) + b'\n'
        nbytes = len(body)
        return nbytes, Metadata({'content-type': 'application/json'}), body
    
    def _sql_encoded_val(self, k, v):
        enc, dec = self._sql_codecs.get(k, (lambda x: x, lambda x: x))
        return enc(v)
    
    def _sql_decoded_val(self, k, v):
        enc, dec = self._sql_codecs.get(k, (lambda x: x, lambda x: x))
        return dec(v)

    def _http_encoded_val(self, k, v):
        enc, dec = self._http_codecs.get(k, (lambda x: x, lambda x: x))
        return enc(v)
    
    def _http_decoded_val(self, k, v):
        enc, dec = self._http_codecs.get(k, (lambda x: x, lambda x: x))
        return dec(v)
    
    def to_sql(self):
        return json.dumps(
            {
                k: self._sql_encoded_val(k, v)
                for k, v in self.items()
            }
        )

    def to_http(self):
        return {
            k: self._http_encoded_val(k, v)
            for k, v in self.items()
        }

    @staticmethod
    def from_sql(orig):
        md = Metadata()
        for k, v in orig.items():
            md[k] = md._sql_decoded_val(k, v)
        return md

    @staticmethod
    def from_http(orig):
        md = Metadata()
        for k, v in orig.items():
            if v is not None:
                md[k] = md._http_decoded_val(k, v.strip())
        return md
    
    def __getitem__(self, k):
        k = k.lower()
        if k not in self._all_keys:
            raise BadRequest('Unknown metadata key %s' % k)
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise NotFound(
                'Metadata %s;metadata/%s not found.' % (self.resource, k)
            )

    def __setitem__(self, k, v):
        k = k.lower()
        if k not in self._all_keys:
            raise BadRequest('Unknown metadata key %s' % k)
        if k in self._write_once_keys and isinstance(v, bytes):
            # HACK: checksums are write-once, stored in memory as bytes, and externalized as base64...
            v = MetadataBytes(v)
        else:
            v = MetadataValue(v)
        v.container = self
        if k in self._write_once_keys and k in self:
            raise Conflict(
                'Metadata %s;metadata/%s cannot be modified once set.' % (self.resource, k)
            )
        dict.__setitem__(self, k.lower(), v)
        if hasattr(self, 'resource'):
            v.resource = self.resource

    def update(self, updates):
        for k, v in updates.items():
            k = k.lower()
            if k not in self or self[k] != v:
                self[k.lower()] = v
            
    def pop(self, k):
        k = k.lower()
        return dict.pop(self, k)


class Redirect(object):
    def __init__(self, url):
        assert url
        self.redirect_url = url

    @property
    def url(self):
        return self.redirect_url

