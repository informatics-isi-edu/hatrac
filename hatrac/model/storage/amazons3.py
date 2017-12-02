#
# Copyright 2015-2017 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
import tempfile
from webauthn2.util import PooledConnection
import boto3
from botocore.exceptions import ClientError
from hatrac.core import BadRequest, coalesce
import binascii
import base64
import logging


class PooledS3BucketConnection (PooledConnection):

    def __init__(self, config):
        """Represent a pool of S3 connections and bucket handles.

           Config is a web.storage object with keys:
              s3_bucket: name of bucket

        """
        self.bucket_name = config['s3_bucket']
        self.object_prefix = config['s3_object_prefix']

        # TODO: encode better identifiers if we ever support multiple
        # buckets or credentials and need separate sub-pools
        self.config_tuple = (self.bucket_name, self.object_prefix)
        PooledConnection.__init__(self, self.config_tuple)

    def _new_connection(self):
        """Open a new S3 connection and get bucket handle."""
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)

        return s3, bucket


def s3_bucket_wrap(deferred_conn_reuse=False):
    """Decorate a method with pooled bucket access.

       If deferred_conn_reuse is True, wrapped method must put back
       (conn, bucket) pair into pool itself, e.g. at end of lazy data
       generator.
    """
    def decorator(orig_method):
        def wrapper(*args, **kwargs):
            self = args[0]
            conn_tuple = None
            try:
                conn_tuple = self._get_pooled_connection()
                conn, bucket = conn_tuple
                try:
                    kwargs1 = dict(kwargs)
                    kwargs1['s3_conn'] = conn
                    kwargs1['s3_bucket'] = bucket
                    return orig_method(*args, **kwargs1)
                    # TODO: catch and map S3 exceptions into hatrac.core.* exceptions?
                except ClientError as s3_error:
                    logging.error("S3 client error: %s", s3_error, exc_info=True)
                    raise BadRequest(s3_error)
                except Exception:
                    conn_tuple = None
                    raise
            finally:
                if conn_tuple and not deferred_conn_reuse:
                    self._put_pooled_connection(conn_tuple)
        return wrapper
    return decorator


class HatracStorage (PooledS3BucketConnection):
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

    _bufsize = 1024**2

    def __init__(self, config):
        """Represents an Hatrac storage interface backed by an S3 bucket.

           See PooledS3Connection for config content documentation.
        """
        PooledS3BucketConnection.__init__(self, config)

    def _prefixed_name(self, name):
        name = name.lstrip("/")
        if self.object_prefix:
            name = "%s/%s" % (self.object_prefix, name)
        return name

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, metadata={}, s3_conn=None, s3_bucket=None):
        """Create an entire file-version object from input content, returning version ID."""
        s3_obj = s3_bucket.Object(self._prefixed_name(name))

        def helper(inp, content_length, md5, content_type):
            response = s3_obj.put(Body=inp,
                                  ContentType=content_type,
                                  ContentLength=content_length,
                                  ContentMD5=md5[1])
            return response['VersionId']

        return self._send_content_from_stream(input, nbytes, metadata, helper)

    def _send_content_from_stream(self, input, nbytes, metadata, sendfunc):
        """Common file-sending logic to talk to S3."""
        content_type = metadata.get('content-type', 'application/octet-stream')
        if 'content-md5' in metadata:
            content_md5 = metadata['content-md5']
            md5 = (binascii.hexlify(content_md5), base64.b64encode(content_md5))
            inp = InputWrapper(input, nbytes)
        else:
            # let S3 backend use a temporary file to rewind and calculate MD5 if needed
            tmpf = tempfile.TemporaryFile()
            md5 = None

            rbytes = 0
            while True:
                if nbytes is not None:
                    buf = input.read(min(self._bufsize, nbytes - rbytes))
                else:
                    buf = input.read(self._bufsize)

                blen = len(buf)
                rbytes += blen
                tmpf.write(buf)

                if blen == 0:
                    if nbytes is not None and rbytes < nbytes:
                        raise IOError('received %d of %d expected bytes' % (rbytes, nbytes))
                    break

            tmpf.seek(0)
            inp = tmpf

        return sendfunc(inp, nbytes, md5, content_type=content_type)

    def get_content(self, name, version, metadata={}):
        return self.get_content_range(name, version, metadata, None)
     
    @s3_bucket_wrap(deferred_conn_reuse=True)
    def get_content_range(self, name, version, metadata={}, get_slice=None, s3_conn=None, s3_bucket=None):
        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_obj.VersionId = version.strip()
        nbytes = s3_obj.content_length
        
        if get_slice is not None:
            pos = coalesce(get_slice.start, 0)
            limit = coalesce(get_slice.stop, nbytes)
        else:
            pos = 0
            limit = nbytes
        
        if pos != 0 or limit != nbytes:
            content_range = 'bytes=%d-%d' % (pos, limit)
        else:
            content_range = 'bytes=0-'

        length = limit - pos

        response = s3_obj.get(Range=content_range, VersionId=version)

        def data_generator():
            conn_tuple = (s3_conn, s3_bucket)
            try:
                for chunk in iter(lambda: response['Body'].read(self._bufsize), b''):
                    yield chunk
            except Exception as ev:
                # discard connections which had errors?
                conn_tuple = None
            finally:
                if conn_tuple:
                    self._put_pooled_connection(conn_tuple)

        return length, metadata, data_generator()

    @s3_bucket_wrap()
    def delete(self, name, version, s3_conn=None, s3_bucket=None):
        """Delete object version."""
        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_obj.VersionId = version.strip()
        s3_obj.delete()

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, metadata={}, s3_conn=None, s3_bucket=None):
        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_upload = s3_obj.initiate_multipart_upload(
            ContentType=metadata.get('content-type', 'application/octet-stream'))
        return s3_upload.id

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes, metadata={},
                               s3_conn=None, s3_bucket=None):

        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_upload = s3_obj.MultipartUpload(upload_id)

        def helper(input, nbytes, md5, content_type=None):
            part = s3_upload.Part(position + 1)
            response = part.upload(Body=input, ContentLength=nbytes)
            return dict(etag=response['ETag'])
        
        return self._send_content_from_stream(input, nbytes, metadata, helper)
              
    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, s3_conn=None, s3_bucket=None):
        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_upload = s3_obj.MultipartUpload(upload_id)
        s3_upload.abort()
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, metadata={}, s3_conn=None, s3_bucket=None):
        s3_obj = s3_bucket.Object(self._prefixed_name(name))
        s3_upload = s3_obj.MultipartUpload(upload_id)
        parts = list()
        for item in iter(chunk_data):
            parts.append({'PartNumber': item['position'] + 1, 'ETag': item['aux']['etag']})
        upload = s3_upload.complete(MultipartUpload={'Parts': parts})
        return upload.version_id

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass


class InputWrapper:
    """Input stream file-like wrapper for uploading data to S3. 

    This module wraps mod_wsgi_input providing implementations of
    seek and tell that are used by boto (but not relied upon)

    """

    def __init__(self, ip, nbytes):
        self._mod_wsgi_input = ip
        self.nbytes = nbytes
        self.reading_started = False
        self.current_position = 0

    def read(self, size=None):
        if self.current_position != 0:
            raise Exception("Stream seek position not at 0")
        self.reading_started = True
        return self._mod_wsgi_input.read(size)

    def tell(self):
        if self.reading_started: 
            raise Exception("Stream reading started")
        
        return self.current_position

    def seek(self, offset, whence=0):
        if self.reading_started: 
            raise Exception("Stream reading started")
        
        if whence == 0:
            if offset > self.nbytes or offset < 0:
                raise IOError("Can't seek beyond stream length")
            self.current_position = offset
        elif whence == 1:
            if offset + self.current_position > self.nbytes or offset + self.current_position < 0:
                raise IOError("Can't seek beyond stream length")
            self.current_position = self.current_position + offset
        else:
            if offset > 0 or self.nbytes + offset < 0:
                raise IOError("Can't seek beyond stream length")
            self.current_position = self.nbytes + offset
             
    def name(self):
        return self._mod_wsgi_input.name()
