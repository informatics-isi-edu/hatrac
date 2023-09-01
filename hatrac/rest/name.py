
#
# Copyright 2015-2022 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Service logic for Hatrac REST API named resources.

"""

from flask import request, make_response, g as hatrac_ctx

from .. import core
from . import app
from .core import RestHandler, \
    NoMethod, Conflict, BadRequest, NotFound, LengthRequired, PayloadTooLarge, \
    hash_list, hatrac_debug

class NameVersion (RestHandler):
    """Represent Hatrac resources addressed by version-qualified names.

    """
    def __init__(self):
        RestHandler.__init__(self)

    # client cannot specify version during PUT so no PUT method...

    def delete(self, name, version, path=''):
        """Destroy object version."""
        resource = self.resolve_version(
            path, name, version
        )
        self.set_http_etag(resource.version)
        self.http_check_preconditions('DELETE')
        resource.delete(
            hatrac_ctx.webauthn2_context
        )
        return self.delete_response()

    def get(self, name, version, path='/'):
        """Get object version."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve_version(
            path, name, version
        )
        self.set_http_etag(resource.version)
        self.http_check_preconditions()
        body, status, headers = self.get_content(
            resource,
            hatrac_ctx.webauthn2_context
        )
        if isinstance(body, core.Redirect):
            return self.redirect_response(body)
        # we need to build the response or flask discards the content-length
        resp = make_response(body, status)
        for k, v in headers.items():
            resp.headers[k] = v
        return resp

_NameVersion_view = app.route(
    '/<hstring:name>:<hstring:version>'
)(app.route(
    '/<hpath:path>/<hstring:name>:<hstring:version>'
)(NameVersion.as_view('NameVersion')))


class NameVersions (RestHandler):
    """Represent Hatrac resources addressed by name and versions sub-resource.

    """
    _namespace_content_type = 'application/x-hatrac-namespace'

    def __init__(self):
        RestHandler.__init__(self)

    def get(self, name, path="/"):
        """Get version listing."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve(
            path, name
        ).get_versions()
        # ugly but safe: hash the ordered list of versions as content ETag 
        self.set_http_etag(
            hash_list([ r.asurl() for r in resource.object.directory.object_enumerate_versions(resource.object)])
        )
        self.http_check_preconditions()
        return self.get_content(
            resource,
            hatrac_ctx.webauthn2_context
        )


_NameVersions_view = app.route(
    '/<hstring:name>;versions'
)(app.route(
    '/<hpath:path>/<hstring:name>;versions'
)(NameVersions.as_view('NameVersions')))


class Name (RestHandler):
    """Represent Hatrac resources addressed by bare names.

    """
    _namespace_content_type = 'application/x-hatrac-namespace'

    def __init__(self):
        RestHandler.__init__(self)

    def put(self, name="", path="/"):
        """Create object version or empty zone."""
        in_content_type = self.in_content_type()
        
        resource = self.resolve(path, name, False)
        if not resource:
            # TODO: clarify disambiguation rules
            if in_content_type == self._namespace_content_type:
                is_object = False
            else:
                is_object = True

            make_parents = request.args.get('parents', 'false').lower() == 'true'
        
            # check precondition for current state of resource not existing
            self.http_check_preconditions('PUT', False)
            resource = hatrac_ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                is_object,
                make_parents,
                hatrac_ctx.webauthn2_context
            )
        elif not resource.is_object():
            self.set_http_etag(
                hash_list([ r.asurl() for r in resource.directory.namespace_enumerate_names(resource, False, False)])
            )
            self.http_check_preconditions('PUT')
            resource.enforce_acl(['owner'], hatrac_ctx.webauthn2_context)
            raise Conflict('Namespace %s already exists.' % resource)
        else:
            try:
                # check preconditions for current state of version existing
                version = resource.get_current_version()
                self.set_http_etag(version.version)
                self.http_check_preconditions('PUT')
            except core.Conflict:
                # check precondition for current state of version not existing
                self.http_check_preconditions('PUT', False)

        # covers update of existing object or first version of new object
        if resource.is_object():
            try:
                nbytes = int(request.environ['CONTENT_LENGTH'])
            except:
                raise LengthRequired()

            if nbytes > core.config.get("max_request_payload_size", core.max_request_payload_size_default):
                raise PayloadTooLarge()

            metadata = { 'content-type': in_content_type }
            
            if 'HTTP_CONTENT_MD5' in request.environ:
                metadata['content-md5'] = request.environ.get('HTTP_CONTENT_MD5').strip()

            if 'HTTP_CONTENT_SHA256' in request.environ:
                metadata['content-sha256'] = request.environ.get('HTTP_CONTENT_SHA256').strip()

            if 'HTTP_CONTENT_DISPOSITION' in request.environ:
                metadata['content-disposition'] = request.environ.get('HTTP_CONTENT_DISPOSITION').strip()

            resource = resource.create_version_from_file(
                request.stream,
                hatrac_ctx.webauthn2_context,
                nbytes,
                metadata=hatrac_ctx.hatrac_directory.metadata_from_http(metadata)
            )
                
        return self.create_response(resource)

    def delete(self, name="", path="/"):
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
            except core.Conflict:
                # check preconditions with no version existing
                self.http_check_preconditions('DELETE', False)
        else:
            # check preconditions on namespace
            self.set_http_etag(
                hash_list([ r.asurl() for r in resource.directory.namespace_enumerate_names(resource, False, False)])
            )
            self.http_check_preconditions('DELETE')
        resource.delete(
            hatrac_ctx.webauthn2_context
        )
        return self.delete_response()

    def get(self, name="", path="/"):
        """Get latest object version or zone listing."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve(
            path, name
        )
        if resource.is_object():
            resource = resource.get_current_version()
            self.set_http_etag(resource.version)
        else:
            self.set_http_etag(
                hash_list([ r.asurl() for r in resource.directory.namespace_enumerate_names(resource, False, False)])
            )
        self.http_check_preconditions()
        body, status, headers = self.get_content(
            resource,
            hatrac_ctx.webauthn2_context
        )
        if isinstance(body, core.Redirect):
            return self.redirect_response(response)
        # we need to build the response or flask discards the content-length
        resp = make_response(body, status)
        for k, v in headers.items():
            resp.headers[k] = v
        return resp

_Name_view = app.route(
    '/'
)(app.route(
    '/<hstring:name>'
)(app.route(
    '/<hstring:name>/'
)(app.route(
    '/<hpath:path>/<hstring:name>'
)(app.route(
    '/<hpath:path>/<hstring:name>/'
)(Name.as_view('Name'))))))
