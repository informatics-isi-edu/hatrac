
#
# Copyright 2015-2026 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Service logic for Hatrac REST API bulk management

"""

import json
from typing import NamedTuple, List
from flask import request, make_response, g as hatrac_ctx

from .. import core
from . import app
from .core import RestHandler, \
    NoMethod, Conflict, BadRequest, NotFound, LengthRequired, PayloadTooLarge, \
    hash_list, hatrac_debug

class BulkVersion (RestHandler):
    """Represent Hatrac version table bulk access

    """
    def __init__(self):
        RestHandler.__init__(self)

    def get(self, name="", path="/"):
        """Get version listings page.

        Most parameters are provided by URL query parameter rather than URL routing!
        """
        # Extract URL query parameters
        limit = request.args.get('limit', '100')
        last_id = request.args.get('last_id', None)
        last_modified_at = request.args.get('last_modified_at', None)

        try:
            limit = int(limit)
            if limit < 1:
                raise ValueError('limit must be greater than 0')
        except Exception as e:
            raise core.BadRequest('Invalid URL parameter "limit" = %r: %s' % (limit, e))

        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve(
            path, name,
        ).bulk_version(limit, last_id, last_modified_at)

        self.set_http_etag(
            resource.get_etag_material()
        )
        self.http_check_preconditions()
        return self.get_content(
            resource,
            hatrac_ctx.webauthn2_context,
        )

_BulkVersion_view = app.route(
    '/;bulk/version'
)(app.route(
    '/;bulk/version/'
)(app.route(
    '/<hstring:name>;bulk/version'
)(app.route(
    '/<hstring:name>;bulk/version/'
)(app.route(
    '/<hpath:path>/<hstring:name>;bulk/version'
)(app.route(
    '/<hpath:path>/<hstring:name>;bulk/version/'
)(BulkVersion.as_view('BulkVersion')))))))


class BulkName (RestHandler):
    """Represent Hatrac name table bulk access

    """
    def __init__(self):
        RestHandler.__init__(self)

    def get(self, name="", path="/"):
        """Get name listings page.

        Most parameters are provided by URL query parameter rather than URL routing!
        """
        # Extract URL query parameters
        limit = request.args.get('limit', '100')
        last_id = request.args.get('last_id', None)
        last_modified_at = request.args.get('last_modified_at', None)

        try:
            limit = int(limit)
            if limit < 1:
                raise ValueError('limit must be greater than 0')
        except Exception as e:
            raise core.BadRequest('Invalid URL parameter "limit" = %r: %s' % (limit, e))

        self.get_body = False if request.method == 'HEAD' else True

        resource = self.resolve(
            path, name,
        ).bulk_name(limit, last_id, last_modified_at)

        self.set_http_etag(
            resource.get_etag_material()
        )
        self.http_check_preconditions()
        return self.get_content(
            resource,
            hatrac_ctx.webauthn2_context,
        )

_BulkName_view = app.route(
    '/;bulk/name'
)(app.route(
    '/;bulk/name/'
)(app.route(
    '/<hstring:name>;bulk/name'
)(app.route(
    '/<hstring:name>;bulk/name/'
)(app.route(
    '/<hpath:path>/<hstring:name>;bulk/name'
)(app.route(
    '/<hpath:path>/<hstring:name>;bulk/name/'
)(BulkName.as_view('BulkName')))))))
