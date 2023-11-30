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
import urllib.parse
from botocore.exceptions import ClientError
from flask import g as hatrac_ctx
from ...core import hatrac_debug, coalesce, max_request_payload_size_default
from ...core import NotFound, BadRequest, Conflict, Redirect, ObjectVersionMissing
from .filesystem import make_random_version


#boto3.set_stream_logger('', level='DEBUG')


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


def explode_path(p):
    """Return list of path elements with empty list for root path"""
    p = p.strip('/')
    return p.split('/') if p else []


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


class BucketTree (object):
    def __init__(self):
        self.children = {}
        self.bucket_config = None

    def digest(self, *path_items, path_context='/', legacy_mode=False):
        """Build out tree structure for given path_items

        :param path_items: zero or more pairs ([prefix, ...], BucketConfig)
        :param path_context: path str of ancestor tree nodes
        :param legacy_mode: limit tree to depth=1 if True
        """
        for path, bucket_config in path_items:
            if not path:
                if self.bucket_config is not None:
                    hatrac_debug('WARNING: ignoring duplicate path mapping for path_context: %r' % (path_context,))
                    continue
                self.bucket_config = bucket_config
                continue

            subtree = self.children.setdefault(path[0], BucketTree())
            subtree.digest(
                (path[1:] if not legacy_mode else [], bucket_config),
                path_context=('%s%s/' % (path_context, path[0])),
            )


class BucketConfigMapper (object):
    """Represent queryable configration to find bucket config for a path."""

    def __init__(self, buckets_config, s3_default_session, legacy_mode=False):
        """Digest hatrac config.s3_config into mapper

        :param buckets_config: projected buckets config from hatrac_config JSON
        :param s3_default_session: default boto s3 session config
        :param legacy_mode: shallow depth=1 path mapping if True
        """
        self.bucket_tree = BucketTree()
        self.bucket_tree.digest(
            *[ (explode_path(prefix), BucketConfig(bucket_config, s3_default_session))
               for prefix, bucket_config in buckets_config.items() ],
            legacy_mode=legacy_mode,
        )

    def get_bucket_config(self, hatrac_object_name):
        """Return bucket_config appropriate for given hatrac_object_name."""
        object_path = explode_path(hatrac_dirname(hatrac_object_name))
        # find most specific tree node matching object path
        subtree = self.bucket_tree
        last_with_bucket = subtree
        for prefix in object_path:
            if prefix not in subtree.children:
                break
            subtree = subtree.children[prefix]
            if subtree.bucket_config is not None:
                last_with_bucket = subtree
        if last_with_bucket.bucket_config is None:
            raise ValueError('Invalid bucket mapping, bucket indeterminate for object: %r' % (hatrac_object_name))
        return last_with_bucket.bucket_config


HatracS3Method = namedtuple(
    "HatracS3Method",
    [
        "expose_s3_version",
        "s3_name_template",
    ]
)


class BucketConfig (object):
    """Represent one bucket configuration

    This configuration is long-lived and may be reused for many s3 object operations.
    """
    s3_methods = {
        "pref/**/hname": HatracS3Method(
            True,
            "%(bucket_prefix)s/%(hatrac_object_name)s",
        ),
        "pref/**/hname:hver": HatracS3Method(
            False,
            "%(bucket_prefix)s/%(hatrac_object_name)s:%(hatrac_object_version)s",
        ),
    }

    def __init__(self, bucket_config, s3_default_session):
        # parse dict once and have other code use these attributes
        self.bucket_name = bucket_config.get("bucket_name")
        if not self.bucket_name:
            raise ValueError("Invalid bucket configuration, missing required key: bucket_name")
        self.bucket_prefix = bucket_config.get("bucket_path_prefix", "hatrac").strip("/")
        self.s3_method_name = bucket_config.get("hatrac_s3_method", "pref/**/hname")
        if self.s3_method_name not in self.s3_methods:
            raise ValueError("Invalid bucket configuration, unknown hatrac_s3_method: %r" % (self.s3_method_name,))
        self.s3_method = self.s3_methods[self.s3_method_name]
        self.unquote_object_keys = bool(bucket_config.get("unquote_object_keys", False))
        self.presigned_url_threshold = \
            bucket_config.get("presigned_url_threshold", bucket_config.get("presigned_url_size_threshold"))
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

    def object_key(self, hatrac_object_name, hatrac_object_version=None):
        key = (self.s3_method.s3_name_template % dict(
            bucket_prefix=self.bucket_prefix,
            hatrac_object_name=hatrac_object_name.lstrip('/'),
            hatrac_object_version=hatrac_object_version,
        )).lstrip('/')
        if self.unquote_object_keys:
            key = urllib.parse.unquote(key)
        return key

    def enforce_versioning_enabled(self):
        bucket_versioning = self.client.get_bucket_versioning(Bucket=self.bucket_name)
        if bucket_versioning.get("Status") != "Enabled":
            raise Conflict(
                "Bucket versioning is required for bucket %s but it is not currently enabled." % self.bucket_name
            )

    def boto_kwargs(self, Bucket=True, **kwargs):
        res = { k: v for k, v in kwargs.items() if v is not None }
        if Bucket is True:
            res['Bucket'] = self.bucket_name
        if not self.s3_method.expose_s3_version:
            res.pop('VersionId', None)
        return res

    def preflight_hatrac_version(self):
        if self.s3_method.expose_s3_version:
            self.enforce_versioning_enabled()
            return None
        else:
            return make_random_version()

    def postflight_hatrac_version(self, preflight_version, s3_response):
        if self.s3_method.expose_s3_version:
            return s3_response['VersionId']
        else:
            return preflight_version

    def postflight_hatrac_upload(self, preflight_version, s3_response):
        upload = s3_response["UploadId"]
        if self.s3_method.expose_s3_version:
            return upload
        else:
            # we need to remember both parts
            return '%s.%s' % (preflight_version, upload)

    def unpack_upload_version(self, upload_id):
        if not self.s3_method.expose_s3_version:
            # we packed these in create_upload() results
            parts = upload_id.split('.')
            version = parts[0]
            upload_id = '.'.join(parts[1:])
        else:
            version = None
        return (upload_id, version)


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
        legacy_mode = self.s3_config.get('legacy_mapping', False)
        self.bucket_mapper = BucketConfigMapper(
            buckets_config,
            self.s3_default_session,
            legacy_mode=legacy_mode,
        )

    @s3_bucket_wrap()
    def create_from_file(self, name, input, nbytes, metadata={}, bucket_config=None):
        """Create an entire file-version object from input content, returning version ID."""
        def sendfunc(inp, content_length, md5, content_type, content_disposition=None):
            version = bucket_config.preflight_hatrac_version()
            response = bucket_config.client.put_object(**bucket_config.boto_kwargs(
                Key=bucket_config.object_key(name, version),
                Body=inp,
                ContentType=content_type,
                ContentLength=content_length,
                ContentDisposition=content_disposition,
                ContentMD5=md5[1].decode() if md5 else None
            ))
            return bucket_config.postflight_hatrac_version(version, response)

        return self._send_content_from_stream(input, nbytes, metadata, sendfunc)

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

    def s3_head_object(self, name, version, s3_version, bucket_config):
        try:
            return bucket_config.client.head_object(**bucket_config.boto_kwargs(
                Key=bucket_config.object_key(name, version),
                VersionId=s3_version,
            ))
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False

    @s3_bucket_wrap()
    def get_content_range(self, name, version, metadata={},
                          get_slice=None, aux={}, version_nbytes=None, bucket_config=None):
        s3_version = aux.get("version") if aux else None
        version_id = version.strip() if not s3_version else s3_version.strip()
        if version_nbytes is None:
            response = self.s3_head_object(name, version, s3_version, bucket_config)
            nbytes = response["ContentLength"]
        else:
            nbytes = version_nbytes

        if bucket_config.over_threshold(nbytes) and not get_slice:
            url = bucket_config.client.generate_presigned_url(
                ClientMethod='get_object',
                ExpiresIn=bucket_config.presigned_url_expiration_secs,
                Params=bucket_config.boto_kwargs(
                    Key=bucket_config.object_key(name, version),
                    VersionId=version_id,
                )
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
            content_range = 'bytes=%d-%d' % (pos, limit - 1)
        else:
            content_range = 'bytes=0-'

        if pos != 0 or limit != nbytes:
            # most object metadata does not apply to partial read content
            metadata = {
                k: v
                for k, v in metadata.items()
                if k in {'content-type'}
            }

        length = limit - pos

        try:
            response = bucket_config.client.get_object(**bucket_config.boto_kwargs(
                Key=bucket_config.object_key(name, version),
                Range=content_range,
                VersionId=version_id,
            ))
        # these matter for overlay backend scenarios
        except bucket_config.client.exceptions.NoSuchKey as e:
            raise ObjectVersionMissing(e)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchVersion':
                raise ObjectVersionMissing(e)
            if e.response['Error']['Code'] == 'InvalidArgument' \
               and e.response['Error']['ArgumentName'] == 'versionId':
                raise ObjectVersionMissing(e)
            hatrac_debug('got unexpected ClientError in amazons3 get_content_range', e, type(e), e.response)
            raise

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
        try:
            response = bucket_config.client.delete_object(**bucket_config.boto_kwargs(
                Key=bucket_config.object_key(name, version),
                VersionId=version_id,
            ))
        # these matter for overlay backend scenarios
        except bucket_config.client.exceptions.NoSuchKey as e:
            raise ObjectVersionMissing(e)
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidArgument' \
               and e.response['Error']['ArgumentName'] == 'versionId':
                raise ObjectVersionMissing(e)
            #hatrac_debug('got unexpected ClientError in amazons3 delete', e, type(e), e.response)
            raise

    @s3_bucket_wrap()
    def create_upload(self, name, nbytes=None, metadata={}, bucket_config=None):
        # thread version state needed for _some_ naming schemes
        version = bucket_config.preflight_hatrac_version()
        response = bucket_config.client.create_multipart_upload(**bucket_config.boto_kwargs(
            Key=bucket_config.object_key(name, version),
            ContentType=metadata.get('content-type', 'application/octet-stream'),
            ContentDisposition=metadata.get('content-disposition', '')
        ))
        return bucket_config.postflight_hatrac_upload(version, response)

    @s3_bucket_wrap()
    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes,
                               metadata={}, bucket_config=None):

        def helper(inp, length, md5, content_type=None, content_disposition=None):
            upload, version = bucket_config.unpack_upload_version(upload_id)
            response = bucket_config.client.upload_part(**bucket_config.boto_kwargs(
                Key=bucket_config.object_key(name, version),
                UploadId=upload,
                PartNumber=position + 1,
                Body=inp,
                ContentLength=length,
            ))
            return dict(etag=response['ETag'])

        return self._send_content_from_stream(input, nbytes, metadata, helper, chunksize)

    @s3_bucket_wrap()
    def cancel_upload(self, name, upload_id, bucket_config=None):
        upload, version = bucket_config.unpack_upload_version(upload_id)
        bucket_config.client.abort_multipart_upload(**bucket_config.boto_kwargs(
            Key=bucket_config.object_key(name, version),
            UploadId=upload
        ))
        return None

    @s3_bucket_wrap()
    def finalize_upload(self, name, upload_id, chunk_data, metadata={}, bucket_config=None):
        upload, version = bucket_config.unpack_upload_version(upload_id)
        response = bucket_config.client.complete_multipart_upload(**bucket_config.boto_kwargs(
            Key=bucket_config.object_key(name, version),
            UploadId=upload,
            MultipartUpload={
                'Parts': [
                    {'PartNumber': item['position'] + 1, 'ETag': item['aux']['etag']}
                    for item in iter(chunk_data)
                ]
            },
        ))
        return bucket_config.postflight_hatrac_version(version, response)

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        # nothing to do for S3 since namespaces are not explicit resources in bucket
        pass

    @s3_bucket_wrap()
    def purge_all_multipart_uploads(self, name, bucket_config=None):
        # NOTE: this only works with the naming method using hatrac names as s3 object keys
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
