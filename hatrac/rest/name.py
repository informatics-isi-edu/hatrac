
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Service logic for Hatrac REST API named resources.

"""

import base64
from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound, LengthRequired, hash_list
import web

@web_url([
     # path, name, version
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+)'
])
class NameVersion (RestHandler):
    """Represent Hatrac resources addressed by version-qualified names.

    """
    def __init__(self):
        RestHandler.__init__(self)

    # client cannot specify version during PUT so no PUT method...

    @web_method()
    def DELETE(self, path, name, version):
        """Destroy object version."""
        resource = self.resolve_version(
            path, name, version
        )
        self.set_http_etag(resource.version)
        self.http_check_preconditions('DELETE')
        resource.delete(
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    # see core.RestHandler.GET and HEAD...
    def _GET(self, path, name, version):
        """Get object version."""
        resource = self.resolve_version(
            path, name, version
        )
        self.set_http_etag(resource.version)
        self.http_check_preconditions()
        return self.get_content(
            resource,
            web.ctx.webauthn2_context
        )

@web_url([
     # path, name
    '/((?:[^/:;]+/)*)([^/:;]+);versions?'
])
class NameVersions (RestHandler):
    """Represent Hatrac resources addressed by name and versions sub-resource.

    """
    _namespace_content_type = 'application/x-hatrac-namespace'

    def __init__(self):
        RestHandler.__init__(self)

    # see core.RestHandler.GET and HEAD...
    def _GET(self, path, name):
        """Get version listing."""
        resource = self.resolve(
            path, name
        ).get_versions()
        # ugly but safe: hash the ordered list of versions as content ETag 
        self.set_http_etag(
            hash_list(resource.object.directory.object_enumerate_versions(resource.object))
        )
        self.http_check_preconditions()
        return self.get_content(
            resource,
            web.ctx.webauthn2_context
        )

@web_url([
     # path, name
    '/((?:[^/:;]+/)*)([^/:;]+)/?',
    '/()()'
])
class Name (RestHandler):
    """Represent Hatrac resources addressed by bare names.

    """
    _namespace_content_type = 'application/x-hatrac-namespace'

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def PUT(self, path, name):
        """Create object version or empty zone."""
        in_content_type = self.in_content_type()

        resource = self.resolve(path, name, False)
        if not resource:
            # TODO: clarify disambiguation rules
            if in_content_type == self._namespace_content_type:
                is_object = False
            else:
                is_object = True

            # check precondition for current state of resource not existing
            self.http_check_preconditions('PUT', False)
            resource = web.ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                is_object,
                web.ctx.webauthn2_context
            )
        elif not resource.is_object():
            self.set_http_etag(
                hash_list(map(str, resource.directory.namespace_enumerate_names(resource, False)))
            )
            self.http_check_preconditions('PUT')
            if in_content_type == self._namespace_content_type:
                # treat this like idempotent create or update?
                resource.enforce_acl(['owner'], web.ctx.webauthn2_context)
                return self.update_response(resource)
            else:
                raise Conflict('Namespace %s does not support update with content-type %s.' % (resource, in_content_type)
        else:
            try:
                # check preconditions for current state of version existing
                version = resource.get_current_version()
                self.set_http_etag(version.version)
                self.http_check_preconditions('PUT')
            except Conflict:
                # check precondition for current state of version not existing
                self.http_check_preconditions('PUT', False)

        # covers update of existing object or first version of new object
        if resource.is_object():
            try:
                nbytes = int(web.ctx.env['CONTENT_LENGTH'])
            except:
                raise LengthRequired()
            if 'HTTP_CONTENT_MD5' in web.ctx.env:
                content_md5 = base64.b64decode(web.ctx.env.get('HTTP_CONTENT_MD5').strip())
            else:
                content_md5 = None
            resource = resource.create_version_from_file(
                web.ctx.env['wsgi.input'],
                web.ctx.webauthn2_context,
                nbytes,
                content_type=in_content_type,
                content_md5=content_md5
            )
                
        return self.create_response(resource)

    @web_method()
    def DELETE(self, path, name):
        """Destroy all object versions or empty zone."""
        resource = self.resolve(
            path, name
        )
        if resource.is_object():
            try:
                # check preconditions against current version
                version = resource.get_current_version()
                self.set_http_etag(version.version)
                self.http_check_preconditions('DELETE')
            except Conflict:
                # check preconditions with no version existing
                self.http_check_preconditions('DELETE', False)
        else:
            # check preconditions on namespace
            self.set_http_etag(
                hash_list(map(str, resource.directory.namespace_enumerate_names(resource, False)))
            )
            self.http_check_preconditions('DELETE')
        resource.delete(
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    # see core.RestHandler.GET and HEAD...
    def _GET(self, path, name):
        """Get latest object version or zone listing."""
        resource = self.resolve(
            path, name
        )
        if resource.is_object():
            resource = resource.get_current_version()
            self.set_http_etag(resource.version)
        else:
            self.set_http_etag(
                hash_list(map(str, resource.directory.namespace_enumerate_names(resource, False)))
            )
        self.http_check_preconditions()
        return self.get_content(
            resource,
            web.ctx.webauthn2_context
        )

