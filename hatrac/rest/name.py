
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Service logic for Hatrac REST API named resources.

"""

from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound, LengthRequired
import web

@web_url([
     # path, name, version
    '/([^/:;]+/)*([^/:;]+):([^/:;]+)'
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
        self.resolve_version(
            path, name, version
        ).delete(
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    @web_method()
    def GET(self, path, name, version):
        """Get object version."""
        resource = self.resolve_version(
            path, name, version
        )
        return self.get_content(
            resource,
            web.ctx.webauthn2_context
        )

@web_url([
     # path, name
    '/([^/:;]+/)*([^/:;]+)',
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

            resource = web.ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                is_object,
                web.ctx.webauthn2_context
            )
        elif not resource.is_object():
            raise NoMethod('Namespace %s does not support PUT requests.' % resource)

        # covers update of existing object or first version of new object
        if resource.is_object():
            try:
                nbytes = int(web.ctx.env['CONTENT_LENGTH'])
            except:
                raise LengthRequired()
            if 'CONTENT_MD5' in web.ctx.env:
                content_md5 = web.ctx.env.get('CONTENT_MD5').lower()
            else:
                content_md5 = None
            resource = resource.create_version_from_file(
                web.ctx.env['wsgi.input'],
                nbytes,
                web.ctx.webauthn2_context,
                content_type=in_content_type,
                content_md5=content_md5
            )
                
        return self.create_response(resource)

    @web_method()
    def DELETE(self, path, name):
        """Destroy all object versions or empty zone."""
        self.resolve(
            path, name
        ).delete(
            web.ctx.webauthn2_context
        )
        return self.delete_response()

    @web_method()
    def GET(self, path, name):
        """Get latest object version or zone listing."""
        resource = self.resolve(
            path, name
        )
        return self.get_content(
            resource,
            web.ctx.webauthn2_context
        )

