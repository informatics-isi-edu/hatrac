
#
# Copyright 2015-2023 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
import os
import hashlib
import base64
import binascii
import random
import struct
import io

from ...core import BadRequest, Conflict, ObjectVersionMissing, coalesce

def construct_with_lazy_import(config):
    """Instantiate the appropriate storage backend for config, with lazy module loading.

    This emulates the same lazy loading done in hatrac.model.storage
    for the top-level config.
    """
    backend = config.get('storage_backend')

    if backend == 'filesystem':
        from . import filesystem
        cls = filesystem.HatracStorage
    elif backend == 'amazons3':
        from . import amazons3
        cls = amazons3.HatracStorage
    elif backend == 'overlay':
        cls = HatracStorage
    else:
        raise ValueError('Invalid configuration: unknown storage_backend %r' % (backend,))

    return cls(config)


class HatracStorage (object):
    """Implement HatracStorage API as a proxy to overlay other backends.

    This overlay is meant to support TEST environments where a hatrac
    DB is cloned to a test server, provisioning a test-specific
    primary storage backend for new writes while allowing existing
    object-versions to be retrieved from a secondary backend shared
    with the original non-test server.

    This hack allows for limited "copy on write" semantics where the
    test environment diverges from the original cloned system at the
    point it is re-provisioned with a snapshot of the original hatrac
    DB. Write traffic to the test server will modify its hatrac DB and
    its own primary storage backend, without affecting the shared
    storage from the non-test system.

    For maximum safety, provision your test server to have read-only
    permissions on secondary backend storage resources. This way, even
    with hatrac bugs or config errors, you will not accidentally
    modify the shared content from the original server.

    The list of storage configurations is treated as a prioritized
    sequence of backends to instantiate and search for files. Each
    config should contain the necessary subset of a global hatrac
    config to parameterize the backend. The first element is the
    primary storage backend and all others are secondary, as in this
    excerpt from a hatrac_config.json file:

    {
      ...
      "storage_backend": "overlay",
      "overlay_backends": [
        {
          "comment": "this is primary",
          "storage_backend": "filesystem",
          "storage_path": "/var/www/hatrac"
        },
        {
          "comment": "this is secondary",
          "storage_backend": "amazons3",
          "s3_config": { ... }
        }
      ]
      ...
    }

    Reads are attempted in the listed backend order until the first
    successful content retrieval. All write requests go to the primary
    backend only.

    Note, concurrent delete operations on the original server can
    cause an inconsistency where the test server's DB references
    object versions which can no longer be retrieved. Such
    inconsistencies may surface as a 500 Internal Server Error
    in GET requests on the lost versions in the test server.

    """
    def __init__(self, config):
        self.configs = config.get('overlay_backends', [])
        if not self.configs \
           or not isinstance(self.configs, list):
            raise ValueError("Invalid configration: overlay_backends must be a list of one or more storage configurations.")
        self.backends = [
            construct_with_lazy_import(config)
            for config in self.configs
        ]

    # method params are passed by position from hatrac.model.directory.pgsql
    # except where specifically proxied as param=value below

    def create_from_file(self, name, input, nbytes, metadata={}):
        return self.backends[0].create_from_file(name, input, nbytes, metadata)

    def create_upload(self, name, nbytes=None, metadata={}):
        return self.backends[0].create_upload(name, nbytes, metadata)

    def cancel_upload(self, name, upload_id):
        return self.backends[0].cancel_upload(name, upload_id)

    def finalize_upload(self, name, upload_id, chunk_data, metadata={}):
        return self.backends[0].finalize_upload(name, upload_id, chunk_data, metadata)

    def upload_chunk_from_file(self, name, upload_id, position, chunksize, input, nbytes, metadata={}, f=None):
        return self.backends[0].upload_chunk_from_file(name, upload_id, position, chunksize, input, nbytes, metadata=metadata)
               
    def get_content(self, name, version, metadata={}, aux={}):
        return self.get_content_range(name, version, metadata, aux=aux)
     
    def get_content_range(self, name, version, metadata={}, get_slice=None, aux={}, nbytes=None):
        if get_slice is None:
            # HACK: supplying a slice will prevent amazons3 backends from using signed-url redirects
            # so we can get a synchronous data result or an ObjectversionMissing exception
            get_slice = slice(0, nbytes)
        for backend in self.backends:
            try:
                return backend.get_content_range(name, version, metadata, get_slice, aux, nbytes)
            except ObjectVersionMissing as e:
                # this is expected for overlay scenario, so try next backend...
                pass
        raise ObjectVersionMissing('Could not locate object version %r:%r' % (name, version))

    def delete(self, name, version, aux={}):
        try:
            return self.backends[0].delete(name, version, aux=aux)
        except ObjectVersionMissing as e:
            # this is expected if the client deletes a version not in primary storage
            pass

    def delete_namespace(self, name):
        # this opportunistic cleanup only applies to the filesystem backend in practice
        return self.backends[0].delete_namespace(name)
