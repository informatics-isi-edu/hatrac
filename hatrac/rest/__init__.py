
#
# Copyright 2015-2019 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import re
import itertools
import web
import urllib

import hatrac.core
from . import core

# these modify core.dispatch_rules
from . import acl
from . import metadata
from . import name
from . import transfer

rules = list(core.dispatch_rules.items())

# sort longest patterns first where prefixes match
rules.sort(reverse=True)

# flatten list of pairs into one long tuple for web.py
rules = [
    # anchor pattern to end of string too for full matching
    (re.compile(pattern + (pattern[-1] != '$' and '$' or '')), handler)
    for pattern, handler in rules
]

read_only = hatrac.core.config.get("read_only", False)


class Dispatcher (object):
    """Helper class to handle parser-based URL dispatch

       Does what mod_wsgi + web.py should do if the standards around
       url-escaping weren't horribly broken.

    """

    def prepare_dispatch(self):
        """computes web dispatch from REQUEST_URI
        """
        uri = web.ctx.env['REQUEST_URI']
        uribase = web.ctx.env['SCRIPT_NAME']
        assert uri[0:len(uribase)] == uribase
        uri = uri[len(uribase):]

        for pattern, handler in rules:
            m = pattern.match(uri)
            if m:
                return handler(), m.groups()
        raise core.NotFound('%s does not map to any REST API.' % uri)

    def METHOD(self, methodname):
        handler, matchgroups = self.prepare_dispatch()
        matchgroups = map(urllib.parse.unquote, matchgroups)

        if not hasattr(handler, methodname):
            raise core.NoMethod()

        method = getattr(handler, methodname)
        return method(*matchgroups)

    def HEAD(self):
        return self.METHOD('HEAD')

    def GET(self):
        return self.METHOD('GET')
        
    def PUT(self):
        if read_only:
            raise core.NoMethod("System is currently in read-only mode.")
        return self.METHOD('PUT')

    def DELETE(self):
        if read_only:
            raise core.NoMethod("System is currently in read-only mode.")
        return self.METHOD('DELETE')

    def POST(self):
        if read_only:
            raise core.NoMethod("System is currently in read-only mode.")
        return self.METHOD('POST')

    # This is for CORS preflight checks that may happen in browsers when Hatrac redirects to another server
    def OPTIONS(self):
        web.ctx.status = '200 OK'
        return ''


# bypass web.py URI-dispatching because of broken handling of url-escapes!
urls = ('.*', Dispatcher)

