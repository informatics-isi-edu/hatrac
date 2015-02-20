
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound
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
        self.resolve_name_or_version(
            path, name, version
        ).set_acl_role(
            access, 
            role, 
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def DELETE(self, path, name, version, access, role):
        """Remove entry from ACL."""
        self.resolve_name_or_version(
            path, name, version
        ).drop_acl_role(
            access, 
            role, 
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    @web_method()
    def GET(self, path, name, version, access, role):
        """Get entry from ACL."""
        resource = self.resolve_name_or_version(path, name, version)
        if access not in resource.acls:
            raise BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        resource.enforce_acl(['owner'], web.ctx.webauthn2_context)
        if role not in resource.acls[access]:
            raise NotFound('ACL member %s;acl/%s/%s not found.' % (resource, access, role))
        web.ctx.status = '200 OK'
        web.header('Content-Length', len(role) + 1)
        web.header('Content-Type', 'text/plain')
        return role + '\n'

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
        self.resolve_name_or_version(
            path, name, version
        ).set_acl(
            access,
            acl,
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def DELETE(self, path, name, version, access):
        """Clear ACL."""
        self.resolve_name_or_version(
            path, name, version
        ).clear_acl(
            access,
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def GET(self, path, name, version, access):
        """Get ACL."""
        resource = self.resolve_name_or_version(path, name, version)
        if access not in resource.acls:
            raise BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        resource.enforce_acl(['owner'], web.ctx.webauthn2_context)
        body = jsonWriterRaw(resource.get_acl(access)) + '\n'
        web.ctx.status = '200 OK'
        web.header('Content-Length', len(body))
        web.header('Content-Type', 'application/json')
        return body
        

@web_url([
    # path, name, version
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);acl/?',
    '/((?:[^/:;]+/)*)([^/:;]+)();acl/?',
    '/()()();acl/?'
])
class ACLs (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def GET(self, path, name, version):
        """Get ACLs."""
        resource = self.resolve_name_or_version(path, name, version)
        resource.enforce_acl(['owner'], web.ctx.webauthn2_context)
        body = jsonWriterRaw(resource.get_acls()) + '\n'
        web.ctx.status = '200 OK'
        web.header('Content-Length', len(body))
        web.header('Content-Type', 'application/json')
        return body

