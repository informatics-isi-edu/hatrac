
#
# Copyright 2015-2022 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import json
from flask import request, g as hatrac_ctx

from . import app
from .core import RestHandler, \
    NoMethod, Conflict, NotFound, BadRequest, \
    hash_list, hash_dict

class ACLEntry (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def put(self, access, role, path="/", name="", version=""):
        """Add entry to ACL."""
        self.enforce_firewall('manage_acl')
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('PUT', role in resource.acls[access])
        resource.set_acl_role(
            access, 
            role, 
            hatrac_ctx.webauthn2_context
        )
        return self.update_response()

    def delete(self, access, role, path="/", name="", version=""):
        """Remove entry from ACL."""
        self.enforce_firewall('manage_acl')
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('DELETE', role in resource.acls[access])
        resource.drop_acl_role(
            access, 
            role, 
            hatrac_ctx.webauthn2_context
        )
        return self.delete_response()

    def get(self, access, role, path="/", name="", version=""):
        """Get entry from ACL."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve_name_or_version(path, name, version).acls[access]
        self.set_http_etag(hash_list(resource))
        resource = resource[role]
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_ACLEntry_view = app.route(
    '/;acl/<access>/<role>'
)(app.route(
    '/<name>;acl/<access>/<role>'
)(app.route(
    '/<name>:<version>;acl/<access>/<role>'
)(app.route(
    '/<path:path>/<name>;acl/<access>/<role>'
)(app.route(
    '/<path:path>/<name>:<version>;acl/<access>/<role>'
)(ACLEntry.as_view('ACLEntry'))))))


class ACL (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def put(self, access, path="/", name="", version=""):
        """Replace ACL."""
        self.enforce_firewall('manage_acl')
        in_content_type = self.in_content_type()
        if in_content_type != 'application/json':
            raise BadRequest('Only application/json input is accepted for ACLs.')
        try:
            acl = json.loads(request.stream.read().decode())
        except:
            raise BadRequest('Error reading JSON input.')
        if not isinstance(acl, list):
            raise BadRequest('ACL input must be a flat JSON array.')
        for entry in acl:
            if not isinstance(entry, str):
                raise BadRequest('ACL entry "%s" is not a string.' % (entry,))
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('PUT')
        resource.set_acl(
            access,
            acl,
            hatrac_ctx.webauthn2_context
        )
        return self.update_response()

    def delete(self, access, path="/", name="", version=""):
        """Clear ACL."""
        self.enforce_firewall('manage_acl')
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('DELETE')
        resource.clear_acl(
            access,
            hatrac_ctx.webauthn2_context
        )
        return self.update_response()

    def get(self, access, path="/", name="", version=""):
        """Get ACL."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve_name_or_version(path, name, version).acls[access]
        self.set_http_etag(hash_list(resource))
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_ACL_view = app.route(
    '/;acl/<access>'
)(app.route(
    '/;acl/<access>/'
)(app.route(
    '/<name>;acl/<access>'
)(app.route(
    '/<name>;acl/<access>/'
)(app.route(
    '/<name>:<version>;acl/<access>'
)(app.route(
    '/<name>:<version>;acl/<access>/'
)(app.route(
    '/<path:path>/<name>;acl/<access>'
)(app.route(
    '/<path:path>/<name>;acl/<access>/'
)(app.route(
    '/<path:path>/<name>:<version>;acl/<access>'
)(app.route(
    '/<path:path>/<name>:<version>;acl/<access>/'
)(ACL.as_view('ACL')))))))))))


class ACLs (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def get(self, path="/", name="", version=""):
        """Get ACLs."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve_name_or_version(path, name, version).acls
        self.set_http_etag(hash_dict(resource))
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_ACLs_view = app.route(
    '/;acl'
)(app.route(
    '/;acl/'
)(app.route(
    '/<name>;acl'
)(app.route(
    '/<name>;acl/'
)(app.route(
    '/<name>:<version>;acl'
)(app.route(
    '/<name>:<version>;acl/'
)(app.route(
    '/<path:path>/<name>;acl'
)(app.route(
    '/<path:path>/<name>;acl/'
)(app.route(
    '/<path:path>/<name>:<version>;acl'
)(app.route(
    '/<path:path>/<name>:<version>;acl/'
)(ACLs.as_view('ACLs')))))))))))
