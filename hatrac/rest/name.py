
#
# Copyright 2015-2025 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Service logic for Hatrac REST API named resources.

"""

import json
from typing import NamedTuple, List
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
        self.enforce_firewall('delete')
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
        while resource.aux.get('rename_to'):
            name, version = resource.aux['rename_to']
            try:
                resource = self.resolve_version(
                    name, '', version
                )
            except core.NotFound as e:
                raise Conflict('Object %s was renamed to %s:%s which no longer exists.' % (resource, name, version))
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

class ObjectRenameCommand (NamedTuple):
    command: str
    source_name: str
    source_versions: List[ str ] = []
    copy_acls: bool = False

def ObjectRenameCommand_from_json(doc):
    """Do some runtime validation of input doc (i.e. decoded JSON) to instantiate a rename command"""
    # HACK: convert @classmethod to standalone function to bypass issues with older python versions
    cls = ObjectRenameCommand
    if not isinstance(doc, dict):
        raise ValueError('Input must be an object (a.k.a. dictionary or hash-mapping).')
    extra_keys = set(doc.keys()).difference(set(cls._fields))
    if extra_keys:
        raise ValueError('Unexpected field(s): %r' % (', '.join(extra_keys),))
    missing_keys = set(cls._fields).difference(cls._field_defaults.keys()).difference(doc.keys())
    if missing_keys:
        raise ValueError('Missing required field(s): %r' % (', '.join(missing_keys),))
    res = cls(**doc)
    field_idx = { cls._fields[i]: i for i in range(len(cls._fields)) }
    for k, t in cls.__annotations__.items():
        v = res[field_idx[k]]
        if isinstance(t, type):
            if not isinstance(v, t):
                raise ValueError('Field %r value %r must be a %s' % (k, v, {'str': 'string', 'bool': 'boolean'}[t.__name__]))
        elif isinstance(t, list) and len(t) == 1:
            if not isinstance(v, list):
                raise ValueError('Field %r value %r must be a list' % (k, v))
            for e in v:
                if not isinstance(e, t[0]):
                    raise ValueError('Field %r element %r must be a %s' % (k, e, {'str': 'string'}[t[0].__name__]))
    return res

class Name (RestHandler):
    """Represent Hatrac resources addressed by bare names.

    """
    _namespace_content_type = 'application/x-hatrac-namespace'

    def __init__(self):
        RestHandler.__init__(self)

    def post(self, name="", path="/"):
        """Perform advanced object-creation command

        """
        self.enforce_firewall('create')
        in_content_type = self.in_content_type()

        if in_content_type != 'application/json':
            raise core.BadRequest('POST method requires application/json object-creation command input.')

        # TODO: refactor if we add other command modes in the future...
        try:
            cmd_doc = json.loads(request.stream.read().decode())
            cmd = ObjectRenameCommand_from_json(cmd_doc)
        except ValueError as ev:
            raise core.BadRequest('Error reading JSON input: %s' % ev)

        if cmd.command != 'rename_from':
            raise core.BadRequest('Invalid command input. Field "command" value %r not understood.' % (cmd.command,))

        src_object = self.resolve(cmd.source_name, '', False)
        if not src_object or not src_object.is_object():
            raise core.Conflict('Request input field "source_name"=%r must name an existing object.' % (cmd.source_name,))

        resource = self.resolve(path, name, False)
        if not resource:
            make_parents = request.args.get('parents', 'false').lower() == 'true'

            # check precondition for current state of resource not existing
            self.http_check_preconditions('PUT', False)
            resource = hatrac_ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                True, # is object
                make_parents,
                hatrac_ctx.webauthn2_context
            )
        elif not resource.is_object():
            raise core.Conflict('Existing namespace cannot be a target for advanced object-creation commands.')
        else:
            try:
                # check preconditions for current state of version existing
                version = resource.get_current_version()
                self.set_http_etag(version.version)
                self.http_check_preconditions('PUT')
            except core.Conflict:
                # check precondition for current state of version not existing
                self.http_check_preconditions('PUT', False)

        resources = resource.rename_from(
            src_object,
            cmd.source_versions,
            cmd.copy_acls,
            hatrac_ctx.webauthn2_context
        )
        return self.create_multi_response(resources)

    def put(self, name="", path="/"):
        """Create object version or empty zone."""
        self.enforce_firewall('create')
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
            resource.enforce_acl(['owner'])
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
        self.enforce_firewall('delete')
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
            while resource.aux.get('rename_to'):
                name, version = resource.aux['rename_to']
                try:
                    resource = self.resolve_version(
                        name, '', version
                    )
                except core.NotFound as e:
                    raise Conflict('Object %s was renamed to %s:%s which no longer exists.' % (resource, name, version))
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
            return self.redirect_response(body)
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
