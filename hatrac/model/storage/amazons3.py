
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
from webauthn2.util import PooledConnection
import boto
import boto.s3
import boto.s3.key
from hatrac.core import BadRequest, coalesce


class PooledS3Connection (object):

    def __init__(self, config):
        """Represent a pool of S3 connections and bucket handles.

           Config is a web.storage object with keys:
              s3_bucket: name of bucket

              s3_connection: nested dictionary of parameters passed as
                 boto.S3Connection(**dict(s3_connection))

        """
        self.bucket_name = config['s3_bucket']

        # TODO: do something better for authz than storing secret in our config data!
        self.conn_config = config['s3_connection']

        # TODO: encode better identifiers if we ever support multiple
        # buckets or credentials and need separate sub-pools
        self.config_tuple = (self.bucket_name,)

    def _new_connection(self):
        """Open a new S3 connection and get bucket handle."""
        conn = boto.S3Connection(**self.conn_config)
        bucket = conn.get_bucket(self.bucket_name)
        return (conn, bucket)

def s3_bucket_wrap(deferrered_conn_reuse=False):
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
        headers = {'Content-Length': str(nbytes)}
        if content_type is not None:
            headers['Content-Type'] = content_type
        if content_md5 is not None:
            headers['Content-MD5'] = content_md5
        s3_key.set_contents_from_stream(input, headers=headers, replace=True, md5=md5, size=nbytes)
        return s3_key.version_id

    def get_content(self, name, version, content_md5=None):
        return self.get_content_range(name, version, content_md5, None)
     
    @s3_bucket_wrap(deferred_conn_reuse=True)
    def get_content_range(self, name, version, content_md5=None, get_slice=None, s3_conn=None, s3_bucket=None):
        s3_key = boto.s3.key.Key(s3_bucket, name)
        s3_key.version_id = version

        if get_slice:
            first = get_slice.start
            last = get_slice.stop - 1
            headers = {'Range':'bytes=%d-%d' % (first, last)}
            rbytes = last - first + 1
        else:
            headers = None
            rbytes = None

        s3_key.open_read(headers=headers)

        if rbytes is None:
            rbytes = s3_key.size

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

        return rbytes, s3_key.content_type, s3_key.md5, data_generator()

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
        upload.id = upload_id
        s3_key = upload.upload_part_from_file(input, position, md5=(content_md5, content_md5_b64), size=nbytes)
        return dict(etag=s3_key.etag)
               
    @s3_bucket_wrap()
    def cancel_upload(self, upload_id, s3_conn=None, s3_bucket=None):
        upload = boto.s3.multipart.MultiPartUpload(s3_bucket)
        upload.id = upload_id
        upload.cancel_upload()

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, s3_conn=None, s3_bucket=None):
        upload = boto.s3.multipart.MultiPartUpload(s3_bucket)
        upload.id = upload_id
        # TODO: is chunk_data even necessary...?
        upload = upload.complete_upload()
        return upload.version_id

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass
