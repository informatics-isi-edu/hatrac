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
from collections import namedtuple
from io import BufferedRandom, BytesIO
from botocore.exceptions import ClientError
from hatrac.core import NotFound, BadRequest, Conflict, coalesce, max_request_payload_size_default, Redirect


S3ConnInfo = namedtuple("S3ConnInfo", ["bucket_name", "object_name", "bucket_config", "client"])


class S3BucketConnection:
    def __init__(self, config, session=None):
        """Represent an S3 Bucket (and its underlying S3 low-level client) connection"""
        self.config = config
        self.session = None
        session_config = self.config.get("session_config")
        if session_config:
            self.session = boto3.session.Session(**session_config)
        if not self.session:
            self.session = boto3.session.Session() if not session else session

        client_config = config.get("client_config", dict())
        self.client = self.session.client('s3', **client_config)


def s3_bucket_wrap():
    """Decorate a method with S3 connection access.

    """

    def decorator(orig_method):
        def wrapper(*args, **kwargs):
            self = args[0]
            try:
                s3_conn_info = self._map_name(args[1])
                kwargs1 = dict(kwargs)
                kwargs1['s3_conn_info'] = s3_conn_info
                return orig_method(*args, **kwargs1)
                # TODO: catch and map S3 exceptions into hatrac.core.* exceptions?
            except ClientError as s3_error:
                if "hatrac_request_trace" in web.ctx:
                    web.ctx.hatrac_request_trace("S3 client error: %s" % s3_error)
                raise BadRequest(s3_error)
            except Exception:
                raise

        return wrapper

    return decorator


class HatracStorage:
    """Implement HatracStorage API using one or more S3 buckets.

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
        """Represents a Hatrac storage interface backed by S3 bucket(s).

        """
        self.config = config
        self.s3_config = config['s3_config']
        self.s3_default_session = boto3.session.Session(
            **self.s3_config.get('default_session', self.s3_config.get('session', dict())))
        self.s3_bucket_mappings = self.s3_config.get("bucket_mappings", self.s3_config.get('buckets', dict()))
        for bucket_mapping in self.s3_bucket_mappings.values():
            if not bucket_mapping.get("conn"):
                bucket_mapping["conn"] = S3BucketConnection(bucket_mapping, self.s3_default_session)

    def _map_name(self, name):
        object_name = name.lstrip("/")
        path_root = "/" + (object_name.split("/")[0].strip() if "/" in object_name else object_name)
        bucket_mapping = self.s3_bucket_mappings.get(path_root, self.s3_bucket_mappings.get("/"))
        if not bucket_mapping:
            raise NotFound("Could not find a bucket mapping for path: %s" % name)
        bucket_name = bucket_mapping.get("bucket_name")
        if not bucket_name:
            raise ValueError("Invalid bucket configuration, missing required key: bucket_name")
        prefix = bucket_mapping.get("bucket_path_prefix", "hatrac")
        if prefix:
            if not prefix.endswith("/"):
                prefix += "/"
            object_name = "%s%s" % (prefix, object_name)

        return S3ConnInfo(bucket_name,
                          object_name,
                          bucket_mapping,
                          bucket_mapping["conn"].client)

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, metadata={}, s3_conn_info=None):
        """Create an entire file-version object from input content, returning version ID."""
        bucket_versioning = s3_conn_info.client.get_bucket_versioning(Bucket=s3_conn_info.bucket_name)
        if bucket_versioning.get("Status") != "Enabled":
            raise Conflict("Bucket versioning is required for bucket %s but it is not currently enabled." %
                           s3_conn_info.bucket_name)

        def helper(inp, content_length, md5, content_type, content_disposition=None):
            response = s3_conn_info.client.put_object(Key=s3_conn_info.object_name,
                                                      Bucket=s3_conn_info.bucket_name,
                                                      Body=inp,
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
        return self.get_content_range(name, version, metadata, None, aux=aux, version_nbytes=None)

    @s3_bucket_wrap()
    def get_content_range(self, name, version, metadata={},
                          get_slice=None, aux={}, version_nbytes=None, s3_conn_info=None):
        s3_version = aux.get("version") if aux else None
        version_id = version.strip() if not s3_version else s3_version.strip()
        if version_nbytes is None:
            response = s3_conn_info.client.head_object(Key=s3_conn_info.object_name,
                                                       Bucket=s3_conn_info.bucket_name,
                                                       VersionId=version_id)
            nbytes = response["ContentLength"]
        else:
            nbytes = version_nbytes

        over_threshold = False
        presigned_url_threshold = s3_conn_info.bucket_config.get("presigned_url_size_threshold")
        if isinstance(presigned_url_threshold, int) and presigned_url_threshold > 0:
            if nbytes > presigned_url_threshold:
                over_threshold = True

        if over_threshold and not get_slice:
            url = s3_conn_info.client.generate_presigned_url(
                ClientMethod='get_object',
                ExpiresIn=s3_conn_info.bucket_config.get("presigned_url_expiration_secs", 300),
                Params={
                    'Bucket': s3_conn_info.bucket_name,
                    'Key': s3_conn_info.object_name,
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

        response = s3_conn_info.client.get_object(Key=s3_conn_info.object_name,
                                                  Bucket=s3_conn_info.bucket_name,
                                                  Range=content_range,
                                                  VersionId=version_id)

        def data_generator(conn_info):
            try:
                for chunk in iter(lambda: response['Body'].read(self._bufsize), b''):
                    yield chunk
            except Exception as ev:
                if "hatrac_request_trace" in web.ctx:
                    web.ctx.hatrac_request_trace("S3 read error: %s" % ev)

        return length, metadata, data_generator(s3_conn_info)

    @s3_bucket_wrap()
    def delete(self, name, version, aux={}, s3_conn_info=None):
        """Delete object version."""
        s3_version = aux.get("version") if aux else None
        version_id = version.strip() if not s3_version else s3_version.strip()
        response = s3_conn_info.client.delete_object(Key=s3_conn_info.object_name,
                                                     Bucket=s3_conn_info.bucket_name,
                                                     VersionId=version_id)

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, metadata={}, s3_conn_info=None):
        response = s3_conn_info.client.create_multipart_upload(
            Key=s3_conn_info.object_name,
            Bucket=s3_conn_info.bucket_name,
            ContentType=metadata.get('content-type', 'application/octet-stream'),
            ContentDisposition=metadata.get('content-disposition', ''))
        return response["UploadId"]

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes,
                               metadata={}, s3_conn_info=None):

        def helper(inp, length, md5, content_type=None, content_disposition=None):
            response = s3_conn_info.client.upload_part(Key=s3_conn_info.object_name,
                                                       Bucket=s3_conn_info.bucket_name,
                                                       UploadId=upload_id,
                                                       PartNumber=position + 1,
                                                       Body=inp,
                                                       ContentLength=length)
            return dict(etag=response['ETag'])

        return self._send_content_from_stream(input, nbytes, metadata, helper, chunksize)

    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, s3_conn_info=None):
        s3_conn_info.client.abort_multipart_upload(Key=s3_conn_info.object_name,
                                                   Bucket=s3_conn_info.bucket_name,
                                                   UploadId=upload_id)
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, metadata={}, s3_conn_info=None):
        parts = list()
        for item in iter(chunk_data):
            parts.append({'PartNumber': item['position'] + 1, 'ETag': item['aux']['etag']})
        response = s3_conn_info.client.complete_multipart_upload(Key=s3_conn_info.object_name,
                                                                 Bucket=s3_conn_info.bucket_name,
                                                                 UploadId=upload_id,
                                                                 MultipartUpload={'Parts': parts})
        return response["VersionId"]

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass

    @s3_bucket_wrap()
    def purge_all_multipart_uploads(self, name, s3_conn_info=None):
        next_key_marker = None
        while True:
            upload_response = s3_conn_info.client.list_multipart_uploads(Bucket=s3_conn_info.bucket_name,
                                                                         KeyMarker=next_key_marker or "")
            uploads = upload_response.get("Uploads")
            if not uploads:
                return
            for upload in uploads:
                key = upload["Key"]
                upload_id = upload["UploadId"]
                try:
                    s3_conn_info.client.abort_multipart_upload(
                        Bucket=s3_conn_info.bucket_name, Key=key, UploadId=upload_id)
                except Exception as e:
                    sys.stderr.print("Error purging S3 multipart upload for Key [%s] with UploadId [%s]: %s" %
                                     (key, upload_id, e))

            if upload_response["IsTruncated"]:
                next_key_marker = upload_response["NextKeyMarker"]
            else:
                break
