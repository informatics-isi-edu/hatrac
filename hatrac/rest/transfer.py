
#
# Copyright 2015-2016 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import re
import web
import hatrac.core
from core import web_url, web_method, RestHandler, NoMethod, Conflict, NotFound, BadRequest
from webauthn2.util import jsonReader

@web_url([
    # path, name, job, chunk
    '/((?:[^/:;]+/)*)([^/:;]+);upload/([^/:;]+)/([^/:;]+)'
])
class ObjectTransferChunk (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def PUT(self, path, name, job, chunk):
        """Upload chunk of transfer job."""
        try:
            chunk = int(chunk)
        except ValueError:
            raise BadRequest('Invalid chunk number %s.' % chunk)

        if chunk < 0:
            raise BadRequest('Invalid chunk number %s.' % chunk)
        
        try:
            nbytes = int(web.ctx.env['CONTENT_LENGTH'])
        except:
            raise LengthRequired()

        metadata = {}

        for hdr, var in [
                ('content-md5', 'HTTP_CONTENT_MD5'),
                ('content-sha256', 'HTTP_CONTENT_SHA256')
        ]:
            val = web.ctx.env.get(var)
            if val is not None:
                metadata[hdr] = val
                
        upload = self.resolve_upload(path, name, job)
        upload.enforce_acl(['owner'], web.ctx.webauthn2_context)
        self.http_check_preconditions('PUT')
        upload.upload_chunk_from_file(
            chunk, 
            web.ctx.env['wsgi.input'],
            web.ctx.webauthn2_context,
            nbytes,
            web.ctx.hatrac_directory.metadata_from_http(metadata)
        )
        return self.update_response()

@web_url([
    # path, name, job
    '/((?:[^/:;]+/)*)([^/:;]+);upload/([^/:;]+)/?'
])
class ObjectTransfer (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def POST(self, path, name, job):
        """Update status of transfer job to finalize."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions('POST')
        version = upload.finalize(web.ctx.webauthn2_context)
        return self.create_response(version)

    @web_method()
    def DELETE(self, path, name, job):
        """Cancel existing transfer job."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions('DELETE')
        upload.cancel(web.ctx.webauthn2_context)
        return self.update_response()

    def _GET(self, path, name, job):
        """Get status of transfer job."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions()
        return self.get_content(upload, web.ctx.webauthn2_context)

@web_url([
    # path, name
    '/((?:[^/:;]+/)*)([^/:;]+);upload/?[?](.*)',
    '/((?:[^/:;]+/)*)([^/:;]+);upload/?()'
])
class ObjectTransfers (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def POST(self, path, name, querystr):
        """Create a new chunked transfer job."""
        in_content_type = self.in_content_type()

        if in_content_type != 'application/json':
            raise BadRequest('Only application/json input is accepted for upload jobs.')
        try:
            job = jsonReader(web.ctx.env['wsgi.input'].read())
        except ValueError, ev:
            raise BadRequest('Error reading JSON input:' % ev)
        if type(job) != dict:
            raise BadRequest('Job input must be a flat JSON object.')

        try:
            try:
                # backwards-compatibility
                chunksize = int(job['chunk_bytes'])
            except KeyError, ev:
                chunksize = int(job['chunk-length'])
            try:
                # backwards-compatibility
                nbytes = int(job['total_bytes'])
            except KeyError, ev:
                nbytes = int(job['content-length'])
        except KeyError, ev:
            raise BadRequest('Missing required field %s.' % ev)
        except ValueError, ev:
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
            params = self.parse_querystr(querystr)
            make_parents = params.get('parents', 'false').lower() == 'true'
            resource = web.ctx.hatrac_directory.create_name(
                self._fullname(path, name),
                True,  # is_object
                make_parents,
                web.ctx.webauthn2_context
            )
        except hatrac.core.Conflict, ev:
            try:
                resource = self.resolve(path, name).get_uploads()
            except hatrac.core.NotFound, ev:
                raise Conflict('Name %s is not available for use.' % self._fullname(path, name))
                
        # say resource_exists=False as we always create a new one...
        self.http_check_preconditions('POST', False)
        upload = resource.create_version_upload_job(
            chunksize, web.ctx.webauthn2_context, nbytes, web.ctx.hatrac_directory.metadata_from_http(metadata)
        )
        return self.create_response(upload)

    def _GET(self, path, name, querystr):
        """List outstanding chunked transfer jobs."""
        resource = self.resolve(path, name).get_uploads()
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)
    


