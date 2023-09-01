
#
# Copyright 2015-2022 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import sys
import re
import itertools
import urllib
from flask import Flask, request
from werkzeug.routing import BaseConverter

from ..core import config

class HatracPathElemConverter(BaseConverter):
    """Hatrac-specific path elements are more limited than Flask
    """
    regex = '[^?/:;]+'

class HatracPathConverter(BaseConverter):
    """Hatrac-specific path elements are more limited than Flask
    """
    regex = '[^?/:;][^?:;]+'
    weight = 200

def raw_path_app(app_orig, raw_uri_env_key='REQUEST_URI'):
    """Allow routes to distinguish raw reserved chars from escaped ones.
    :param app_orig: The WSGI app to wrap with middleware.
    :param raw_path_env_key: The key to lookup the raw request URI in the WSGI environment.
    """
    def app(environ, start_response):
        parts = urllib.parse.urlparse(environ[raw_uri_env_key])
        path_info = parts.path
        script_name = environ['SCRIPT_NAME']
        if path_info.startswith(script_name):
            path_info = path_info[len(script_name):]
        if parts.params:
            path_info = '%s;%s' % (path_info, parts.params)
        environ['PATH_INFO'] = path_info
        return app_orig(environ, start_response)
    return app

app = Flask(__name__)
app.wsgi_app = raw_path_app(app.wsgi_app)
app.url_map.converters['hstring'] = HatracPathElemConverter
app.url_map.converters['hpath'] = HatracPathConverter

read_only = config.get("read_only", False)
# TODO: add method decorator for this!!

# import these (circularly!) to register app routes
from . import acl
from . import metadata
from . import name
from . import transfer
