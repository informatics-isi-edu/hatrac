
#
# Copyright 2016 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound, BadRequest, hash_value, hash_dict
from webauthn2.util import jsonWriterRaw, jsonReader
import web

@web_url([
    # path, name, version, fieldname
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);metadata/([^/:;]+)',
    '/((?:[^/:;]+/)*)([^/:;]+)();metadata/([^/:;]+)',
    '/()()();metadata/([^/:;]+)'
])
class Metadata (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def PUT(self, path, name, version, fieldname):
        """Replace Metadata value."""
        in_content_type = self.in_content_type()
        if in_content_type != 'text/plain':
            raise BadRequest('Only text/plain input is accepted for metadata.')

        value = web.ctx.env['wsgi.input'].read()

        resource = self.resolve_name_or_version(
            path, name, version
        )
        if not resource.is_object():
            raise NotFound('Namespaces do not have metadata sub-resources.')
        resource = resource.get_current_version()
        
        self.set_http_etag(hash_value(resource.metadata.get(fieldname, '')))
        self.http_check_preconditions('PUT')

        resource.update_metadata(
            web.ctx.hatrac_directory.metadata_from_http({ fieldname: value }),
            web.ctx.webauthn2_context
        )
        return self.update_response()

    @web_method()
    def DELETE(self, path, name, version, fieldname):
        """Clear Metadata value."""
        resource = self.resolve_name_or_version(
            path, name, version
        )
        if not resource.is_object():
            raise NotFound('Namespaces do not have metadata sub-resources.')
        resource = resource.get_current_version()

        self.set_http_etag(hash_value(resource.metadata.get(fieldname, '')))
        self.http_check_preconditions('DELETE')

        resource.pop_metadata(
            fieldname,
            web.ctx.webauthn2_context
        )
        return self.update_response()

    def _GET(self, path, name, version, fieldname):
        """Get Metadata value."""
        resource = self.resolve_name_or_version(path, name, version)
        if not resource.is_object():
            raise NotFound('Namespaces do not have metadata sub-resources.')
        resource = resource.get_current_version().metadata[fieldname]

        self.set_http_etag(hash_value(resource))
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)
        

@web_url([
    # path, name, version
    '/((?:[^/:;]+/)*)([^/:;]+):([^/:;]+);metadata/?',
    '/((?:[^/:;]+/)*)([^/:;]+)();metadata/?',
    '/()()();metadata/?'
])
class MetadataCollection (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def _GET(self, path, name, version):
        """Get Metadata collection."""
        resource = self.resolve_name_or_version(path, name, version)
        if not resource.is_object():
            raise NotFound('Namespaces do not have metadata sub-resources.')
        resource = resource.get_current_version().metadata

        self.set_http_etag(hash_dict(resource))
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)
