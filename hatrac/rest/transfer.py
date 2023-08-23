
#
# Copyright 2015-2022 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import re
import json
from flask import request, g as hatrac_ctx

from . import app
from .. import core
from .core import RestHandler, \
    NoMethod, Conflict, NotFound, BadRequest, LengthRequired, PayloadTooLarge

class ObjectTransferChunk (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def put(self, name, job, chunk, path="/"):
        """Upload chunk of transfer job."""
        try:
            chunk = int(chunk)
        except ValueError:
            raise BadRequest('Invalid chunk number %s.' % chunk)

        if chunk < 0:
            raise BadRequest('Invalid chunk number %s.' % chunk)
        
        try:
            nbytes = int(request.environ['CONTENT_LENGTH'])
        except:
            raise LengthRequired()

        if nbytes > core.config.get("max_request_payload_size", core.max_request_payload_size_default):
            raise PayloadTooLarge()

        metadata = {}

        for hdr, var in [
                ('content-md5', 'HTTP_CONTENT_MD5'),
                ('content-sha256', 'HTTP_CONTENT_SHA256')
        ]:
            val = request.environ.get(var)
            if val is not None:
                metadata[hdr] = val
                
        upload = self.resolve_upload(path, name, job)
        upload.enforce_acl(['owner'], hatrac_ctx.webauthn2_context)
        self.http_check_preconditions('PUT')
        upload.upload_chunk_from_file(
            chunk, 
            request.stream,
            hatrac_ctx.webauthn2_context,
            nbytes,
            hatrac_ctx.hatrac_directory.metadata_from_http(metadata)
        )
        return self.update_response()

    def get(self, name, job, chunk, path="/"):
        # flask raises 404 on GET if get method isn't defined
        # our existing test suite expects resource to exist but raise 405
        raise NoMethod()

_ObjectTransferChunk_view = app.route(
    '/<name>;upload/<job>/<chunk>'
)(app.route(
    '/<path:path>/<name>;upload/<job>/<chunk>'
)(ObjectTransferChunk.as_view('ObjectTransferChunk')))


class ObjectTransfer (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def post(self, name, job, path="/"):
        """Update status of transfer job to finalize."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions('POST')
        version = upload.finalize(hatrac_ctx.webauthn2_context)
        return self.create_response(version)

    def delete(self, name, job, path="/"):
        """Cancel existing transfer job."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions('DELETE')
        upload.cancel(hatrac_ctx.webauthn2_context)
        return self.update_response()

    def get(self, name, job, path="/"):
        """Get status of transfer job."""
        self.get_body = False if request.method == 'HEAD' else True
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions()
        return self.get_content(upload, hatrac_ctx.webauthn2_context)

_ObjectTransfer_view = app.route(
    '/<name>;upload/<job>'
)(app.route(
    '/<name>;upload/<job>/'
)(app.route(
    '/<path:path>/<name>;upload/<job>'
)(app.route(
    '/<path:path>/<name>;upload/<job>/'
)(ObjectTransfer.as_view('ObjectTransfer')))))


class ObjectTransfers (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    def post(self, name, path="/"):
        """Create a new chunked transfer job."""
        in_content_type = self.in_content_type()

        if in_content_type != 'application/json':
            raise BadRequest('Only application/json input is accepted for upload jobs.')
        try:
            job = json.loads(request.stream.read().decode())
        except ValueError as ev:
            raise BadRequest('Error reading JSON input:' % ev)
        if type(job) != dict:
            raise BadRequest('Job input must be a flat JSON object.')

        try:
            try:
                # backwards-compatibility
                chunksize = int(job['chunk_bytes'])
            except KeyError as ev:
                chunksize = int(job['chunk-length'])
            try:
                # backwards-compatibility
                nbytes = int(job['total_bytes'])
            except KeyError as ev:
                nbytes = int(job['content-length'])
        except KeyError as ev:
            raise BadRequest('Missing required field %s.' % ev)
        except ValueError as ev:
            raise BadRequest('Invalid count: %s.' % ev)

        metadata = {}

        for hdr, keys in [
                ('content-type', {'content_type', 'content-type'}),
                ('content-md5', {'content_md5', 'content-md5'}),
                ('content-sha256', {'content-sha256'}),
                ('content-disposition', {'content-disposition'})]:
            for key in keys:
                val = job.get(key)
                if val is not None:
                    metadata[hdr] = val
            
        # create object implicitly or reuse existing object...
        try:
            make_parents = request.args.get('parents', 'false').lower() == 'true'
            resource = hatrac_ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                True,  # is_object
                make_parents,
                hatrac_ctx.webauthn2_context
            )
        except core.Conflict as ev:
            try:
                resource = self.resolve(path, name).get_uploads()
            except core.NotFound as ev:
                raise Conflict('Name %s is not available for use.' % self._fullname(path, name))
                
        # say resource_exists=False as we always create a new one...
        self.http_check_preconditions('POST', False)
        upload = resource.create_version_upload_job(
            chunksize, hatrac_ctx.webauthn2_context, nbytes, hatrac_ctx.hatrac_directory.metadata_from_http(metadata)
        )
        return self.create_response(upload)

    def get(self, name, path="/"):
        """List outstanding chunked transfer jobs."""
        self.get_body = False if request.method == 'HEAD' else True
        resource = self.resolve(path, name).get_uploads()
        self.http_check_preconditions()
        return self.get_content(resource, hatrac_ctx.webauthn2_context)

_ObjectTransfers_view = app.route(
    '/<name>;upload'
)(app.route(
    '/<name>;upload/'
)(app.route(
    '/<path:path>/<name>;upload'
)(app.route(
    '/<path:path>/<name>;upload/'
)(ObjectTransfers.as_view('ObjectTransfers')))))
