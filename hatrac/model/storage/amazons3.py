#
# Copyright 2015-2017 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""AmazonS3-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
import base64
import binascii
import boto3
import sys
import web
from io import BufferedRandom, BytesIO
from webauthn2.util import PooledConnection
from botocore.exceptions import ClientError
from hatrac.core import NotFound, BadRequest, coalesce, max_request_payload_size_default, Redirect


class PooledS3BucketConnection(PooledConnection):

    def __init__(self, config):
        """Represent a pool of S3 connections.

        """
        self.config = config
        self.s3_config = config['s3_config']
        self.s3_session = self.s3_config.get('session', dict())
        self.s3_buckets = self.s3_config.get("buckets", dict())

        self.config_tuple = ("s3_sessions",)
        PooledConnection.__init__(self, self.config_tuple)

    def _new_connection(self):
        """Create a new S3 session."""
        session = boto3.session.Session(**self.s3_session)
        s3 = session.resource('s3')

        return s3


def s3_bucket_wrap(deferred_conn_reuse=False):
    """Decorate a method with S3 session access.

    """

    def decorator(orig_method):
        def wrapper(*args, **kwargs):
            self = args[0]
            s3_session = None
            try:
                s3_session = self._get_pooled_connection()
                try:
                    kwargs1 = dict(kwargs)
                    kwargs1['s3_session'] = s3_session
                    return orig_method(*args, **kwargs1)
                    # TODO: catch and map S3 exceptions into hatrac.core.* exceptions?
                except ClientError as s3_error:
                    if "hatrac_request_trace" in web.ctx:
                        web.ctx.hatrac_request_trace("S3 client error: %s" % s3_error)
                    raise BadRequest(s3_error)
                except Exception:
                    s3_session = None
                    raise
            finally:
                if s3_session and not deferred_conn_reuse:
                    self._put_pooled_connection(s3_session)

        return wrapper

    return decorator


class HatracStorage(PooledS3BucketConnection):
    """Implement HatracStorage API using an S3 bucket.

       A configured storage bucket, object name, and object version
       are combined to form one S3 object reference

         https://bucket.s3.amazonaws.com/ object_name ? versionId=object_version

       consistent with Hatrac rules.  The incoming name may include
       RFC3986 percent-encoded URL characters, which we assume S3 can
       tolerate.

    """
    track_chunks = True

    _bufsize = 1024 ** 2 * 10

    def __init__(self, config):
        """Represents an Hatrac storage interface backed by an S3 bucket.

           See PooledS3Connection for config content documentation.
        """
        PooledS3BucketConnection.__init__(self, config)

    def _map_name(self, name):
        object_name = name.lstrip("/")
        path_root = "/" + (object_name.split("/")[0].strip() if "/" in object_name else object_name)
        bucket = self.s3_buckets.get(path_root, self.s3_buckets.get("/"))
        if not bucket:
            raise NotFound("Could not find a bucket mapping for path: %s" % name)
        bucket_name = bucket.get("bucket_name")
        if not bucket_name:
            raise ValueError("Invalid bucket configuration, missing required key: bucket_name")
        prefix = bucket.get("bucket_path_prefix", "hatrac")
        if prefix:
            if not prefix.endswith("/"):
                prefix += "/"
            object_name = "%s%s" % (prefix, object_name)

        region_name = bucket.get("region_name")
        if region_name and not bucket.get("client"):
            # this client object is used for signing URLs because that operation is directly coupled to bucket region
            # per the boto documentation, the client object is re-entrant
            bucket["client"] = boto3.client("s3", region_name)

        return bucket_name, object_name, bucket

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, metadata={}, s3_session=None):
        """Create an entire file-version object from input content, returning version ID."""
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)

        def helper(inp, content_length, md5, content_type, content_disposition=None):
            response = s3_obj.put(Body=inp,
                                  ContentType=content_type,
                                  ContentLength=content_length,
                                  ContentDisposition=content_disposition or "",
                                  ContentMD5=md5[1].decode() if md5 else "")
            return response['VersionId']

        return self._send_content_from_stream(input, nbytes, metadata, helper)

    def _send_content_from_stream(self, input, nbytes, metadata, sendfunc, chunksize=None):
        """Common file-sending logic to talk to S3."""
        content_type = metadata.get('content-type', 'application/octet-stream')
        content_disposition = metadata.get('content-disposition')
        md5 = None
        if 'content-md5' in metadata:
            content_md5 = metadata['content-md5']
            md5 = (binascii.hexlify(content_md5), base64.b64encode(content_md5))

        rbytes = 0
        rbuf = BufferedRandom(
            BytesIO(), chunksize or self.config.get("max_request_payload_size", max_request_payload_size_default))
        try:
            while True:
                if nbytes is not None:
                    buf = input.read(min(self._bufsize, nbytes - rbytes))
                else:
                    buf = input.read(self._bufsize)

                blen = len(buf)
                rbytes += blen
                rbuf.write(buf)

                if blen == 0:
                    if nbytes is not None and rbytes < nbytes:
                        raise IOError('received %d of %d expected bytes' % (rbytes, nbytes))
                    break
            rbuf.seek(0)
            return sendfunc(rbuf, nbytes, md5, content_type=content_type, content_disposition=content_disposition)
        finally:
            if rbuf:
                rbuf.close()

    def get_content(self, name, version, metadata={}, aux={}):
        return self.get_content_range(name, version, metadata, None, aux=aux)
     
    @s3_bucket_wrap(deferred_conn_reuse=True)
    def get_content_range(self, name, version, metadata={}, get_slice=None, aux={}, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
        s3_version = aux.get("version")
        s3_obj.VersionId = version.strip() if not s3_version else s3_version.strip()
        nbytes = s3_obj.content_length

        over_threshold = False
        presigned_url_threshold = bucket_config.get("presigned_url_size_threshold")
        if isinstance(presigned_url_threshold, int) and presigned_url_threshold > 0:
            if nbytes > presigned_url_threshold:
                over_threshold = True

        if over_threshold and not get_slice:
            client = bucket_config.get("client")
            if not client:
                client = s3_session.meta.client
            url = client.generate_presigned_url(
                ClientMethod='get_object',
                ExpiresIn=bucket_config.get("presigned_url_expiration_secs", 300),
                Params={
                    'Bucket': bucket_name,
                    'Key': object_name,
                    'VersionId': s3_obj.VersionId
                }
            )
            response = Redirect(url)
            return nbytes, metadata, response

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

        response = s3_obj.get(Range=content_range, VersionId=s3_obj.VersionId)

        def data_generator(session):
            try:
                for chunk in iter(lambda: response['Body'].read(self._bufsize), b''):
                    yield chunk
            except Exception as ev:
                session = None
                if "hatrac_request_trace" in web.ctx:
                    web.ctx.hatrac_request_trace("S3 read error: %s" % ev)
            finally:
                if session:
                    self._put_pooled_connection(session)

        return length, metadata, data_generator(s3_session)

    @s3_bucket_wrap()
    def delete(self, name, version, aux={}, s3_session=None):
        """Delete object version."""
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
        s3_version = aux.get("version")
        s3_obj.VersionId = version.strip() if not s3_version else s3_version.strip()
        s3_obj.delete()

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, metadata={}, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
        s3_upload = s3_obj.initiate_multipart_upload(
            ContentType=metadata.get('content-type', 'application/octet-stream'),
            ContentDisposition=metadata.get('content-disposition', ''))
        return s3_upload.id

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes, metadata={}, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
        s3_upload = s3_obj.MultipartUpload(upload_id)

        def helper(input, nbytes, md5, content_type=None, content_disposition=None):
            part = s3_upload.Part(position + 1)
            response = part.upload(Body=input, ContentLength=nbytes)
            return dict(etag=response['ETag'])

        return self._send_content_from_stream(input, nbytes, metadata, helper, chunksize)

    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
        s3_upload = s3_obj.MultipartUpload(upload_id)
        s3_upload.abort()
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, metadata={}, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        s3_bucket = s3_session.Bucket(bucket_name)
        s3_obj = s3_bucket.Object(object_name)
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

    @s3_bucket_wrap()
    def purge_all_multipart_uploads(self, name, s3_session=None):
        bucket_name, object_name, bucket_config = self._map_name(name)
        client = bucket_config.get("client")
        if not client and s3_session is not None:
            client = s3_session.meta.client
        next_key_marker = None
        while True:
            upload_response = client.list_multipart_uploads(Bucket=bucket_name, KeyMarker=next_key_marker or "")
            uploads = upload_response.get("Uploads")
            if not uploads:
                return
            for upload in uploads:
                key = upload["Key"]
                upload_id = upload["UploadId"]
                try:
                    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)
                except Exception as e:
                    if "hatrac_request_trace" in web.ctx:
                        web.ctx.hatrac_request_trace("S3 client error: %s" % e)
                    else:
                        sys.stderr.print("Error purging S3 multipart upload for Key [%s] with UploadId [%s]: %s" %
                                         (key, upload_id, e))

            if upload_response["IsTruncated"]:
                next_key_marker = upload_response["NextKeyMarker"]
            else:
                break
