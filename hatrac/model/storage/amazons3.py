#
# Copyright 2015-2023 University of Southern California
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
import os
from collections import namedtuple
from io import BufferedRandom, BytesIO
from botocore.exceptions import ClientError
from flask import g as hatrac_ctx
from ...core import hatrac_debug, coalesce, max_request_payload_size_default
from ...core import NotFound, BadRequest, Conflict, Redirect


boto3.set_stream_logger('', level='DEBUG')


def s3_bucket_wrap():
    """Decorate a method with S3 connection access.

    """

    def decorator(orig_method):
        def wrapper(*args, **kwargs):
            self = args[0]
            try:
                hatrac_object_name = args[1]
                bucket_config = self.bucket_mapper.get_bucket_config(hatrac_object_name)
                kwargs1 = dict(kwargs)
                kwargs1['bucket_config'] = bucket_config
                return orig_method(*args, **kwargs1)
                # TODO: catch and map S3 exceptions into hatrac.core.* exceptions?
            except ClientError as s3_error:
                if "hatrac_request_trace" in hatrac_ctx:
                    hatrac_ctx.hatrac_request_trace("S3 client error: %s" % s3_error)
                raise BadRequest(s3_error)
            except Exception:
                raise

        return wrapper

    return decorator


def rewrite_path(path):
    """Rewrite a path into a canonical form /path/. """
    path = path.strip('/')
    if path:
        return '/%s/' % (path,)
    else:
        return '/'


def dict_get_first(d, *keys, default=None):
    """Return the value for the first key or return default value."""
    for k in keys:
        if k in d:
            return d[k]
    return default


def hatrac_dirname(name):
    """Similar to os.path.dirname

    Always uses '/' separator regardless of platform.
    Always returns leading and trailing '/'.
    """
    path = '/'.join(name.strip('/').split('/')[0:-1])
    return rewrite_path(path)


class BucketConfigMapper (object):
    """Represent queryable configration to find bucket config for a path."""
    def __init__(self, buckets_config, s3_default_session):
        """Digest hatrac config.s3_config into mapper"""
        self.prefix_map = [
            (rewrite_path(prefix), BucketConfig(bucket_config, s3_default_session))
            for prefix, bucket_config in buckets_config.items()
        ]
        # sort from longest to shortest prefix since we prefer longest matches
        self.prefix_map.sort(key=lambda e: (len(e[0]), e), reverse=True)

    def get_bucket_config(self, hatrac_object_name):
        """Return bucket_config appropriate for given hatrac_object_name."""
        object_path = hatrac_dirname(hatrac_object_name)
        for prefix, bucket_config in self.prefix_map:
            if object_path.startswith(prefix):
                return bucket_config
        raise NotFound("Could not find a bucket configuration for path: %s" % object_path)


class BucketConfig (object):
    """Represent one bucket configuration

    This configuration is long-lived and may be reused for many s3 object operations.
    """
    def __init__(self, bucket_config, s3_default_session):
        # parse dict once and have other code use these attributes
        self.bucket_name = bucket_config.get("bucket_name")
        if not self.bucket_name:
            raise ValueError("Invalid bucket configuration, missing required key: bucket_name")
        self.bucket_prefix = bucket_config.get("bucket_path_prefix", "hatrac").strip("/")
        self.presigned_url_threshold = bucket_config.get("presigned_url_threshold")
        self.presigned_url_expiration_secs = bucket_config.get("presigned_url_expiration_secs", 300)
        if not isinstance(self.presigned_url_threshold, int) \
           or self.presigned_url_threshold <= 0:
            self.presigned_url_threshold = None
        # setup boto s3 client for this bucket
        # memoization to original config seems unnecessary but retain for now?
        self.client = bucket_config.get("s3_boto_client")
        if self.client is None:
            session = s3_default_session
            session_config = bucket_config.get("session_config")
            if session_config:
                session = boto3.session.Session(**session_config)
            if session is None:
                session = boto3.session.Session()
            client_config = bucket_config.get("client_config", dict())
            self.client = session.client("s3", **client_config)
            bucket_config["s3_boto_client"] = self.client

    def over_threshold(self, nbytes):
        if self.presigned_url_threshold is not None:
            if nbytes > self.presigned_url_threshold:
                return True
        return False

    def object_key(self, hatrac_object_name):
        return ("%s/%s" % (self.bucket_prefix, hatrac_object_name.lstrip('/'))).lstrip('/')


class HatracStorage:
    """Implement HatracStorage API using one or more S3 buckets.

       A configured storage bucket, object name, and object version
       are combined to form one S3 object reference

         https://bucket.s3.amazonaws.com/ bucket_path_prefix object_name ? versionId=object_version

       consistent with Hatrac rules.  The incoming name may include
       RFC3986 percent-encoded URL characters, which we assume S3 can
       tolerate.

       The object_name is the full namespace-qualified hatrac object name
       stripped of the /hatrac/ service prefix.

       The bucket_path_prefix is a configurable prefix to add to this
       object name when storing to the bucket, defaulting to hatrac/.
       It can be set to "" or "/" to store just the object_name path
       directly in the root of the bucket.

    This class is instantiated once and reused for the lifetime of the
    service process to handle many storage access requests.

    """
    track_chunks = True

    _bufsize = 1024 ** 2 * 10

    def __init__(self, config):
        """Represents a Hatrac storage interface backed by S3 bucket(s).

        """
        self.config = config
        self.s3_config = config['s3_config']
        self.s3_default_session = boto3.session.Session(
            **self.s3_config.get('default_session', self.s3_config.get('session', dict())))
        buckets_config = dict_get_first(self.s3_config, 'buckets', 'bucket_mappings', default={})
        self.bucket_mapper = BucketConfigMapper(buckets_config, self.s3_default_session)

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, metadata={}, bucket_config=None):
        """Create an entire file-version object from input content, returning version ID."""
        bucket_versioning = bucket_config.client.get_bucket_versioning(Bucket=bucket_config.bucket_name)
        if bucket_versioning.get("Status") != "Enabled":
            raise Conflict("Bucket versioning is required for bucket %s but it is not currently enabled." %
                           bucket_config.bucket_name)

        def helper(inp, content_length, md5, content_type, content_disposition=None):
            response = bucket_config.client.put_object(
                Key=bucket_config.object_key(name),
                Bucket=bucket_config.bucket_name,
                Body=inp,
                ContentType=content_type,
                ContentLength=content_length,
                ContentDisposition=content_disposition or "",
                ContentMD5=md5[1].decode() if md5 else ""
            )
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
        return self.get_content_range(name, version, metadata, None, aux=aux, version_nbytes=None)

    @s3_bucket_wrap()
    def get_content_range(self, name, version, metadata={},
                          get_slice=None, aux={}, version_nbytes=None, bucket_config=None):
        s3_version = aux.get("version") if aux else None
        version_id = version.strip() if not s3_version else s3_version.strip()
        if version_nbytes is None:
            response = bucket_config.client.head_object(
                Key=bucket_config.object_key(name),
                Bucket=bucket_config.bucket_name,
                VersionId=version_id
            )
            nbytes = response["ContentLength"]
        else:
            nbytes = version_nbytes

        if bucket_config.over_threshold(nbytes) and not get_slice:
            url = bucket_config.client.generate_presigned_url(
                ClientMethod='get_object',
                ExpiresIn=bucket_config.presigned_url_expiration_secs,
                Params={
                    'Bucket': bucket_config.bucket_name,
                    'Key': bucket_config.object_key(name),
                    'VersionId': version_id
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

        response = bucket_config.client.get_object(
            Key=bucket_config.object_key(name),
            Bucket=bucket_config.bucket_name,
            Range=content_range,
            VersionId=version_id
        )

        def data_generator(response):
            try:
                for chunk in iter(lambda: response['Body'].read(self._bufsize), b''):
                    yield chunk
            except Exception as ev:
                if "hatrac_request_trace" in hatrac_ctx:
                    hatrac_ctx.hatrac_request_trace("S3 read error: %s" % ev)

        return length, metadata, data_generator(response)

    @s3_bucket_wrap()
    def delete(self, name, version, aux={}, bucket_config=None):
        """Delete object version."""
        s3_version = aux.get("version") if aux else None
        version_id = version.strip() if not s3_version else s3_version.strip()
        response = bucket_config.client.delete_object(
            Key=bucket_config.object_key(name),
            Bucket=bucket_config.bucket_name,
            VersionId=version_id
        )

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, metadata={}, bucket_config=None):
        response = bucket_config.client.create_multipart_upload(
            Key=bucket_config.object_key(name),
            Bucket=bucket_config.bucket_name,
            ContentType=metadata.get('content-type', 'application/octet-stream'),
            ContentDisposition=metadata.get('content-disposition', ''))
        return response["UploadId"]

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes,
                               metadata={}, bucket_config=None):

        def helper(inp, length, md5, content_type=None, content_disposition=None):
            response = bucket_config.client.upload_part(
                Key=bucket_config.object_key(name),
                Bucket=bucket_config.bucket_name,
                UploadId=upload_id,
                PartNumber=position + 1,
                Body=inp,
                ContentLength=length
            )
            return dict(etag=response['ETag'])

        return self._send_content_from_stream(input, nbytes, metadata, helper, chunksize)

    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, bucket_config=None):
        bucket_config.client.abort_multipart_upload(
            Key=bucket_config.object_key(name),
            Bucket=bucket_config.bucket_name,
            UploadId=upload_id
        )
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, metadata={}, bucket_config=None):
        parts = list()
        for item in iter(chunk_data):
            parts.append({'PartNumber': item['position'] + 1, 'ETag': item['aux']['etag']})
        response = bucket_config.client.complete_multipart_upload(
            Key=bucket_config.object_key(name),
            Bucket=bucket_config.bucket_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        return response["VersionId"]

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass

    @s3_bucket_wrap()
    def purge_all_multipart_uploads(self, name, bucket_config=None):
        next_key_marker = None
        while True:
            upload_response = bucket_config.client.list_multipart_uploads(
                Bucket=bucket_config.bucket_name,
                KeyMarker=next_key_marker or ""
            )
            uploads = upload_response.get("Uploads")
            if not uploads:
                return
            for upload in uploads:
                key = upload["Key"]
                upload_id = upload["UploadId"]
                try:
                    bucket_config.client.abort_multipart_upload(
                        Bucket=bucket_config.bucket_name,
                        Key=key,
                        UploadId=upload_id
                    )
                except Exception as e:
                    sys.stderr.print("Error purging S3 multipart upload for Key [%s] with UploadId [%s]: %s" %
                                     (key, upload_id, e))

            if upload_response["IsTruncated"]:
                next_key_marker = upload_response["NextKeyMarker"]
            else:
                break
