
#
# Copyright 2016-2022 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from flask import request, g as hatrac_ctx

from . import app
from .core import RestHandler, \
    NoMethod, Conflict, NotFound, BadRequest, \
    hash_value, hash_dict

class Metadata (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def put(self, fieldname, path="/", name="", version=""):
        """Replace Metadata value."""
        self.enforce_firewall('manage_metadata')
        in_content_type = self.in_content_type()
        if in_content_type != 'text/plain':
            raise BadRequest('Only text/plain input is accepted for metadata.')

        value = request.stream.read().decode()

        if version:
            resource = self.resolve_version(path, name, version)
        else:
            resource = self.resolve(path, name)
            if not resource.is_object():
                raise NotFound('Namespaces do not have metadata sub-resources.')
            resource = resource.get_current_version()
        
        self.set_http_etag(hash_value(resource.metadata.get(fieldname, '')))
        self.http_check_preconditions('PUT')

        resource.update_metadata(
            hatrac_ctx.hatrac_directory.metadata_from_http({ fieldname: value }),
            hatrac_ctx.webauthn2_context
        )
        return self.update_response()

    def delete(self, fieldname, path="/", name="", version=""):
        """Clear Metadata value."""
        self.enforce_firewall('manage_metadata')
        if version:
            resource = self.resolve_version(path, name, version)
        else:
            resource = self.resolve(path, name)
            if not resource.is_object():
                raise NotFound('Namespaces do not have metadata sub-resources.')
            resource = resource.get_current_version()

        self.set_http_etag(hash_value(resource.metadata.get(fieldname, '')))
        self.http_check_preconditions('DELETE')

        resource.pop_metadata(
            fieldname,
            hatrac_ctx.webauthn2_context
        )
        return self.update_response()

    def get(self, fieldname, path="/", name="", version=""):
        """Get Metadata value."""
        self.get_body = False if request.method == 'HEAD' else True
        if version:
            resource = self.resolve_version(path, name, version)
        else:
            resource = self.resolve(path, name)
            if not resource.is_object():
                raise NotFound('Namespaces do not have metadata sub-resources.')
            resource = resource.get_current_version()

        resource = resource.metadata[fieldname]

        self.set_http_etag(hash_value(resource))
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_Metadata_view = app.route(
    '/;metadata/<hstring:fieldname>'
)(app.route(
    '/<hstring:name>;metadata/<hstring:fieldname>'
)(app.route(
    '/<hstring:name>:<hstring:version>;metadata/<hstring:fieldname>'
)(app.route(
    '/<hpath:path>/<hstring:name>;metadata/<hstring:fieldname>'
)(app.route(
    '/<hpath:path>/<hstring:name>:<hstring:version>;metadata/<hstring:fieldname>'
)(Metadata.as_view('Metadata'))))))

class MetadataCollection (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def get(self, path="/", name="", version=""):
        """Get Metadata collection."""
        self.get_body = False if request.method == 'HEAD' else True
        if version:
            resource = self.resolve_version(path, name, version)
        else:
            resource = self.resolve(path, name)
            if not resource.is_object():
                raise NotFound('Namespaces do not have metadata sub-resources.')
            resource = resource.get_current_version()

        resource = resource.metadata

        self.set_http_etag(hash_dict(resource))
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_MetadataCollection_view = app.route(
    '/;metadata'
)(app.route(
    '/;metadata/'
)(app.route(
    '/<hstring:name>;metadata'
)(app.route(
    '/<hstring:name>;metadata/'
)(app.route(
    '/<hstring:name>:<hstring:version>;metadata'
)(app.route(
    '/<hstring:name>:<hstring:version>;metadata/'
)(app.route(
    '/<hpath:path>/<hstring:name>;metadata'
)(app.route(
    '/<hpath:path>/<hstring:name>;metadata/'
)(app.route(
    '/<hpath:path>/<hstring:name>:<hstring:version>;metadata'
)(app.route(
    '/<hpath:path>/<hstring:name>:<hstring:version>;metadata/'
)(MetadataCollection.as_view('MetadataCollection')))))))))))

