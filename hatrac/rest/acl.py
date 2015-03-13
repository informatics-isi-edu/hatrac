
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound, BadRequest, hash_list, hash_dict
from webauthn2.util import jsonWriterRaw, jsonReader
import web

@web_url([
    # path, name, version, access, role
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);acl/([^/:;]+)/([^/:;]+)',
    '/((?:[^/:;]+/)*)([^/:;]+)();acl/([^/:;]+)/([^/:;]+)',
    '/()()();acl/([^/:;]+)/([^/:;]+)'
])
class ACLEntry (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def PUT(self, path, name, version, access, role):
        """Add entry to ACL."""
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('PUT', role in resource.acls[access])
        resource.set_acl_role(
            access, 
            role, 
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def DELETE(self, path, name, version, access, role):
        """Remove entry from ACL."""
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('DELETE', role in resource.acls[access])
        resource.drop_acl_role(
            access, 
            role, 
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    def _GET(self, path, name, version, access, role):
        """Get entry from ACL."""
        resource = self.resolve_name_or_version(path, name, version).acls[access]
        self.set_http_etag(hash_list(resource))
        resource = resource[role]
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)

@web_url([
    # path, name, version, access
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);acl/([^/:;]+)/?',
    '/((?:[^/:;]+/)*)([^/:;]+)();acl/([^/:;]+)/?',
    '/()()();acl/([^/:;]+)/?'
])
class ACL (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def PUT(self, path, name, version, access):
        """Replace ACL."""
        in_content_type = self.in_content_type()
        if in_content_type != 'application/json':
            raise BadRequest('Only application/json input is accepted for ACLs.')
        try:
            acl = jsonReader(web.ctx.env['wsgi.input'])
        except:
            raise BadRequest('Error reading JSON input.')
        if type(acl) != list:
            raise BadRequest('ACL input must be a flat JSON array.')
        for entry in acl:
            if type(acl) != str:
                raise BadRequest('ACL entry "%s" is not a string.' % entry)
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('PUT')
        resource.set_acl(
            access,
            acl,
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def DELETE(self, path, name, version, access):
        """Clear ACL."""
        resource = self.resolve_name_or_version(
            path, name, version
        )
        self.set_http_etag(hash_list(resource.acls[access]))
        self.http_check_preconditions('DELETE')
        resource.clear_acl(
            access,
            web.ctx.webauthn2_context
        )
        return self.update_response()

    def _GET(self, path, name, version, access):
        """Get ACL."""
        resource = self.resolve_name_or_version(path, name, version).acls[access]
        self.set_http_etag(hash_list(resource))
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)
        

@web_url([
    # path, name, version
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);acl/?',
    '/((?:[^/:;]+/)*)([^/:;]+)();acl/?',
    '/()()();acl/?'
])
class ACLs (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def _GET(self, path, name, version):
        """Get ACLs."""
        resource = self.resolve_name_or_version(path, name, version).acls
        self.set_http_etag(hash_dict(resource))
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)

