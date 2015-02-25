
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
from hatrac.core import BadRequest, coalesce

class HatracStorage (object):
    """Implement HatracStorage API using an S3 bucket.

       A configured storage bucket, object name, and object version
       are combined to form one S3 object reference

         https://bucket.s3.amazonaws.com/ object_name ? versionId=object_version

       consistent with Hatrac rules.  The incoming name may include
       RFC3986 percent-encoded URL characters, which we assume S3 can
       tolerate.

    """
    track_chunks = True

    # TODO: decide whether to map to S3 versioning or just encode
    # Hatrac versions directly in S3 object names (seperate S3 object
    # per Hatrac object-version)...  perhaps a config option?

    # TODO: generally raise hatrac.core.* exceptions when things go wrong

    # BUG: hatrac doesn't really expect many storage exceptions and
    # might need new handling to pass transient errors back to client
    # in a way that allows client-driven compensation

    _bufsize = 1024**2

    def __init__(self, config):
        self.bucket = config['s3_bucket']
        # TODO: define and get any other config parameters required to talk to S3

    def create_from_file(self, name, input, nbytes, content_type=None, content_md5=None):
        """Create an entire file-version object from input content, returning version ID."""
        
        raise NotImplementedError()

        REQUEST = """
PUT /%(name)s
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s
Content-Type: %(content_type)s
Content-Length: %(nbytes)d
Content-MD5: %(content_md5)s

...entity...
"""
        # TODO: read nbytes from input and stream into request as request entity

        RESPONSE = """
HTTP/1.1 200 OK
...
X-amz-id-version-id: ...version_id...
...
"""
        # TODO: extract version_id payload from response
        return version_id

    def create_upload(self, name, nbytes=None, content_type=None, content_md5=None):

        raise NotImplementedError()

        REQUEST = """
POST /%(name)s?uploads
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s
Content-Type: %(content_type)s
"""

        RESPONSE = """
HTTP/1.1 200 OK
Content-Length: N

<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>...bucket...</Bucket>
  <Key>...object name...</Key>
  <UploadId>...upload_id...</UploadId>
</InitiateMultipartUploadResult>
"""
        # TODO: extract UploadId element body from response XML
        return upload_id

    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes, content_md5=None, f=None):

        raise NotImplementedError()

        REQUEST = """
PUT /%(name)s?partNumber=%(position)d&uploadId=%(upload_id)s HTTP/1.1
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s
Content-Length: %(nbytes)d
Content-MD5: %(content_md5)s

...entity...
"""
        # TODO: read nbytes from input and stream into request as request entity

        RESPONSE = """
HTTP/1.1 200 OK
...
ETag: "...ETag value..."
...
"""
        # TODO: extract Etag value from chunk response

        # return auxilliary chunk info for directory layer to track for us
        return dict(etag=etag_value)

               
    def get_content(self, name, version, content_md5=None):

        raise NotImplementedError()

        REQUEST = """
GET /%(name)s?versionId=%(version)s
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s
"""

        RESPONSE = """
HTTP/1.1 200 OK
Content-Length: ...nbytes...
Content-Type: ...content_type...
Content-MD5: ...content_md5...

...response entity...
"""
        # TODO extract response headers and construct a
        # data-generating closure that will yield response entity of nbytes length
        # in efficiently buffered reads

        return nbytes, content_type, content_md5, data_generator
     
    def get_content_range(self, name, version, content_md5=None, get_slice=None):

        raise NotImplementedError()

        # TODO: unpack Range first/last if get_slice is not None
        # otherwise behave same as self.get_content() above

        # (convert from Python slice indexing to HTTP Range indexing
        # which uses inclusive limit)
        first = get_slice.start
        last = get_slice.stop - 1

        REQUEST = """
GET /%(name)s?versionId=%(version)s
Host: %(bucket)s.s3.amazonaws.com
Range: bytes=%(first)d-%(last)d
Authorization: %(s3_authz)s
"""

        RESPONSE = """
HTTP/1.1 206 Partial Content
Content-Length: ...nbytes...
Content-Type: ...content_type...
Content-Range: bytes first-last/total
Content-MD5: ...content_md5...

...response entity...
"""
        # TODO extract response headers and construct a
        # data-generating closure that will yield response entity of nbytes length
        # in efficiently buffered reads

        # note nbytes is number of bytes in partial read, not total bytes
        return nbytes, content_type, content_md5, data_generator


    def cancel_upload(self, upload_id):

        raise NotImplementedError()

        REQUEST = """
DELETE /%(name)s?uploadId=%(upload_id)s HTTP/1.1
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s
"""

        RESPONSE = """
HTTP/1.1 204 OK
...
"""


    def finalize_upload(self, name, upload_id, chunk_data):

        raise NotImplementedError()

        # TODO: build request XML entity using chunk_data list

        chunk.sort(key=lambda c: c.position)

        for chunk in chunk_data:
            assert chunk.position is not None
            assert chunk.etag is not None

        REQUEST = """
POST /%(name)s?uploadId=%(upload_id)s HTTP/1.1
Host: %(bucket)s.s3.amazonaws.com
Authorization: %(s3_authz)s

<CompleteMultipartUpload>
  <Part>
    <PartNumber>...part number...</PartNumber>
    <ETag>ETag</ETag>
  </Part>
  ...
</CompleteMultipartUpload>
"""

        RESPOSE = """
HTTP/1.1 200 OK
...
X-amz-version-id: ...version_id...

<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>...object URL...</Location>
  <Bucket>...bucket...</Bucket>
  <Key>...name...</Key>
  <ETag>...ETag...</ETag>
</CompleteMultipartUploadResult>
"""
        # TODO: extract version_id from response header
        return version_id

    def delete(self, name, version):
        """Delete object version."""
        
        raise NotImplementedError()

        REQUEST = """
DELETE /%(name)s?versionId=%(version)s
Host: %(bucket)s.s3.amazonaws.com
"""

        RESPONSE = """
HTTP/1.1 204 No Content
"""
        return None

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass
