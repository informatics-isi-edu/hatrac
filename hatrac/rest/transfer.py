
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import re
import binascii
import base64
import web
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
        try:
            nbytes = int(web.ctx.env['CONTENT_LENGTH'])
        except:
            raise LengthRequired()
        if 'HTTP_CONTENT_MD5' in web.ctx.env:
            content_md5 = base64.b64decode(web.ctx.env.get('HTTP_CONTENT_MD5').strip())
        else:
            content_md5 = None
        upload = self.resolve_upload(path, name, job)
        upload.enforce_acl(['owner'], web.ctx.webauthn2_context)
        self.http_check_preconditions('PUT')
        upload.upload_chunk_from_file(
            chunk, 
            web.ctx.env['wsgi.input'],
            web.ctx.webauthn2_context,
            nbytes,
            content_md5
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
        return self.update_response(version)

    def _GET(self, path, name, job):
        """Get status of transfer job."""
        upload = self.resolve_upload(path, name, job)
        self.http_check_preconditions()
        return self.get_content(upload, web.ctx.webauthn2_context)

@web_url([
    # path, name
    '/((?:[^/:;]+/)*)([^/:;]+);upload/?'
])
class ObjectTransfers (RestHandler):

    def __init__(self):
        RestHandler.__init__(self)

    @web_method()
    def POST(self, path, name):
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
            chunksize = int(job['chunk_bytes'])
            nbytes = int(job['total_bytes'])
            content_type = job.get('content_type')
            content_md5 = job.get('content_md5')
        except KeyError, ev:
            raise BadRequest('Missing required field %s.' % ev)
        except ValueError, ev:
            raise BadRequest('Invalid count: %s.' % ev)

        if content_md5 is not None:
            content_md5 = content_md5.strip()

        if content_md5 is None:
            pass
        elif re.match('^[0-9a-zA-z]\+$', content_md5) and len(content_md5) == 32:
            # tolerate a hex digest
            content_md5 = binascii.unhexlify(content_md5)
        elif len(content_md5) == 24:
            try:
                content_md5 = base64.b64decode(content_md5)
            except Exception, ev:
                raise BadRequest('Invalid content_md5 base64 encoded value: %s.' % ev)
        else:
            raise BadRequest('Invalid content_md5 "%s" is neither 32-byte hex or 24-byte base64 string.' % content_md5)

        resource = self.resolve(path, name).get_uploads()
        # say resource_exists=False as we always create a new one...
        self.http_check_preconditions('POST', False)
        upload = resource.create_version_upload_job(
            chunksize, web.ctx.webauthn2_context, nbytes, content_type, content_md5
        )
        return self.create_response(upload)

    def _GET(self, path, name):
        """List outstanding chunked transfer jobs."""
        resource = self.resolve(path, name).get_uploads()
        self.http_check_preconditions()
        return self.get_content(resource, web.ctx.webauthn2_context)
    


