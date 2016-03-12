
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
import os
import tempfile
from webauthn2.util import PooledConnection
import boto
import boto.s3
import boto.s3.key
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from hatrac.core import BadRequest, coalesce
import binascii
import base64

class PooledS3BucketConnection (PooledConnection):

    def __init__(self, config):
        """Represent a pool of S3 connections and bucket handles.

           Config is a web.storage object with keys:
              s3_bucket: name of bucket

              s3_connection: nested dictionary of parameters passed as
              boto.S3Connection(**dict(s3_connection)), e.g.:

                 aws_access_key_id,
                 aws_secret_access_key,
                 provider,
                 security_token,
                 anon,
                 validate_certs

        """
        self.bucket_name = config['s3_bucket']

        # TODO: do something better for authz than storing secret in our config data!
        self.conn_config = config['s3_connection']

        # TODO: encode better identifiers if we ever support multiple
        # buckets or credentials and need separate sub-pools
        self.config_tuple = (self.bucket_name,)
        PooledConnection.__init__(self, self.config_tuple)

    def _new_connection(self):
        """Open a new S3 connection and get bucket handle."""
        conn = S3Connection(**self.conn_config)
        bucket = conn.get_bucket(self.bucket_name)
        return (conn, bucket)

def s3_bucket_wrap(deferred_conn_reuse=False):
    """Decorate a method with pooled bucket access.

       If deferred_conn_reuse is True, wrapped method must put back
       (conn, bucket) pair into pool itself, e.g. at end of lazy data
       generator.
    """
    def decorator(orig_method):
        def wrapper(*args):
            self = args[0]
            conn_tuple = None
            try:
                conn_tuple = self._get_pooled_connection()
                conn, bucket = conn_tuple
                try:             
                    return orig_method(*args, s3_conn=conn, s3_bucket=bucket)
                except S3ResponseError, s3_error:
                    raise BadRequest(s3_error.message)
                except Exception, ev:
                    # TODO: catch and map S3 exceptions into hatrac.core.* exceptions?
                    conn_tuple = None
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

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, content_type=None, content_md5=None, s3_conn=None, s3_bucket=None):
        """Create an entire file-version object from input content, returning version ID."""
        s3_key = boto.s3.key.Key(s3_bucket, name)
        def helper(input, headers, nbytes, md5):
            s3_key.set_contents_from_file(input, headers, replace=True, md5=md5, size=nbytes, rewind=False)
            return s3_key.version_id
        return self._send_content_from_stream(input, nbytes, content_type, content_md5, helper)

    def _send_content_from_stream(self, input, nbytes, content_type, content_md5, sendfunc):
        """Common file-sending logic to talk to S3."""
        headers = {'Content-Length': str(nbytes)}
        if content_type is not None:
            headers['Content-Type'] = content_type
        if content_md5 is not None:
            md5 = (binascii.hexlify(content_md5), base64.b64encode(content_md5))
            input = InputWrapper(input, nbytes)
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
            input = tmpf
            
        return sendfunc(input, headers, nbytes, md5)

    def get_content(self, name, version, content_md5=None):
        return self.get_content_range(name, version, content_md5, None)
     
    @s3_bucket_wrap(deferred_conn_reuse=True)
    def get_content_range(self, name, version, content_md5=None, get_slice=None, s3_conn=None, s3_bucket=None):
        s3_key = boto.s3.key.Key(s3_bucket, name)
        s3_key.version_id = version.strip()
        
        if get_slice is None:
            headers = None
            rbytes = None
        else:
            headers = {'Range':'bytes=%d-%d' % (get_slice.start, get_slice.stop-1)}
            rbytes = get_slice.stop - get_slice.start
            
        # Note: version ID is not being set on the key, this forces the right version
        s3_key.open_read(headers=headers,query_args='versionId=%s' % version)
       
        md5 = s3_key.md5
        if rbytes is None:
            rbytes = s3_key.size
            md5 = content_md5

        def data_generator():
            conn_tuple = (s3_conn, s3_bucket)
            try:
                for bytes in s3_key:
                    yield bytes
            except Exception, ev:
                # discard connections which had errors?
                conn_tuple = None
            finally:
                if conn_tuple:
                    self._put_pooled_connection(conn_tuple)

        return rbytes, s3_key.content_type, md5, data_generator()

    @s3_bucket_wrap()
    def delete(self, name, version, s3_conn=None, s3_bucket=None):
        """Delete object version."""
        s3_key = boto.s3.key.Key(s3_bucket, name)
        s3_key.version_id = version
        s3_key.delete()

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, content_type=None, content_md5=None, s3_conn=None, s3_bucket=None):
        upload = s3_bucket.initiate_multipart_upload(name)
        return upload.id

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes, content_md5=None, s3_conn=None, s3_bucket=None):
        upload = boto.s3.multipart.MultiPartUpload(s3_bucket)
        upload.key_name = name
        upload.id = upload_id

        def helper(input, headers, nbytes, md5):
            # We have to set content-type to None to avoid boto trying to work out type
            if headers is None or 'Content-Type' not in headers:
                headers['Content-Type'] = None
            s3_key = upload.upload_part_from_file(input, position + 1, headers=headers, md5=md5, size=nbytes)
           
            return dict(etag=s3_key.etag)
        return self._send_content_from_stream(input, nbytes, None, content_md5, helper)

              
    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, s3_conn=None, s3_bucket=None):
        upload = boto.s3.multipart.MultiPartUpload(s3_bucket)
        upload.key_name = name
        upload.id = upload_id
        upload.cancel_upload()
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, s3_conn=None, s3_bucket=None, content_md5=None):
        upload = boto.s3.multipart.MultiPartUpload(s3_bucket)
        upload.key_name = name
        upload.id = upload_id
        # TODO: is chunk_data even necessary...?
        upload = upload.complete_upload()
        return upload.version_id

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass

class InputWrapper():
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
                raise IOException("Can't seek beyond stream lenght")
            self.current_position = offset
        elif whence == 1:
            if offset + self.current_position > self.nbytes or offset + self.current_position < 0:
                raise IOException("Can't seek beyond stream length") 
            self.current_position = self.current_position + offset
        else:
            if offset > 0 or self.nbytes + offset < 0:
                raise IOException("Can't seek beyond stream length")
            self.current_position = self.nbytes + offset
             
    def name(self):
        return self._mod_wsgi_input.name()
