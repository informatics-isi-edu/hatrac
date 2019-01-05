
#
# Copyright 2015-2019 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core module config

"""

import binascii
import base64
import re
import urllib
import web
import json

from webauthn2 import merge_config, jsonWriter

config = merge_config(
    jsonFileName='hatrac_config.json'
)

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
        #web.debug('_string_wrap', s, escape, protect, e)
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
        
        
