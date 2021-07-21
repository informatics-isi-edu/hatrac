
#
# Copyright 2015-2019 University of Southern California
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

from ...core import BadRequest, Conflict, coalesce

def make_file(dirname, relname, accessmode):
    """Create and open file with accessmode, including missing parents.

    Returns fp.

    """
    # TODO: test for conflicts during creation?
    filename = "%s/%s" % (dirname, relname)

    if not os.path.exists(dirname):
        os.makedirs(dirname, mode=0o755)

    return open(filename, accessmode, 0)

class HatracStorage (object):
    """Implement HatracStorage API using basic POSIX filesystem mapping.

       A configured storage rootdir, object name, and object version
       are combined to form one filename to store the immutable
       object:

          / rootdir / object_name : object_version

       consistent with Hatrac rules.  The incoming name may include
       RFC3986 percent-encoded URL characters, which we assume our
       filesystem can tolerate.

    """
    track_chunks = False

    _bufsize = 1024**2

    def __init__(self, config):
        self.root = config.get('storage_path', '/var/www/hatrac')

    def _dirname_relname(self, name, version):
        """Map Hatrac identifiers to backend storage."""
        # TODO: consider hashing if too many namespaces exist at top level
        assert name
        assert version
        assert ':' not in version

        dirname = self.root
        nameparts = [ n for n in name.split('/') if n ]
        dirparts = nameparts[0:-1]
        relpart = nameparts[-1]
        relname = "%s:%s" % (relpart, version)

        assert relpart
        
        if dirparts:
            dirname = "%s/%s" % (self.root, "/".join(dirparts))
        else:
            dirname = self.root

        return (dirname, relname)

    def create_from_file(self, name, input, nbytes, metadata={}):
        """Create an entire file-version object from input content, returning version ID."""
        
        version = base64.b32encode( 
            (struct.pack('Q', random.getrandbits(64))
             + struct.pack('Q', random.getrandbits(64)))[0:26]
        ).decode().replace('=', '') # strip off '=' padding

        dirname, relname = self._dirname_relname(name, version)
        f = make_file(dirname, relname, 'wb')

        # upload whole content at offset 0 (for code reuse)
        self.upload_chunk_from_file(None, None, 0, 0, input, nbytes, metadata, f)
        return version

    def create_upload(self, name, nbytes=None, metadata={}):
        upload_id = self.create_from_file(name, io.BytesIO(b''), 0)
        return upload_id

    def cancel_upload(self, name, upload_id):
        # this backend uses upload_id as version_id
        self.delete(name, upload_id)
        return None

    def finalize_upload(self, name, upload_id, chunk_data, metadata={}):
        # nothing changes in storage for this backend strategy
        version_id = upload_id
        assert chunk_data is None

        # aggressively validate uploaded content against pre-defined MD5 if it was given at job start
        if 'content-md5' in metadata:
            dirname, relname = self._dirname_relname(name, version_id)
            fullname = "%s/%s" % (dirname, relname)
            f = open(fullname, "rb")

            hasher = hashlib.md5()

            eof = False
            while not eof:
                buf = f.read(self._bufsize)
                if len(buf) != 0:
                    hasher.update(buf)
                else:
                    eof = True

            stored_md5 = hasher.digest()
            if metadata['content-md5'] != stored_md5:
                raise Conflict(
                    'Current uploaded content MD5 %s does not match expected %s.'
                    % (binascii.hexlify(stored_md5), binascii.hexlify(metadata['content-md5']))
                )
        
        return version_id

    def upload_chunk_from_file(self, name, version, position, chunksize, input, nbytes, metadata={}, f=None):
        """Save chunk data into storage.

           If self.track_chunks, return value must be None or a value
           that can be serialized using webauthn2.util.jsonWriteRaw,
           i.e. dict, array, or scalar values.

        """
        if f is None:
            dirname, relname = self._dirname_relname(name, version)
            fullname = "%s/%s" % (dirname, relname)
            
            f = open(fullname, "r+b")
        f.seek(position*chunksize)

        if 'content-md5' in metadata:
            hasher = hashlib.md5()
        else:
            hasher = None

        rbytes = 0
        eof = False
        while not eof:
            if nbytes is not None:
                bufsize = min(nbytes-rbytes, self._bufsize)
            else:
                bufsize = self._bufsize

            buf = input.read(bufsize)
            f.write(buf)
            bufsize = len(buf)
            rbytes += bufsize

            if hasher:
                hasher.update(buf)

            if nbytes is not None:
                if rbytes >= nbytes:
                    eof = True
                elif bufsize == 0:
                    f.close()
                    raise BadRequest('Only received %s of %s expected bytes.' % (rbytes, nbytes))
            elif bufsize == 0:
                eof = True

        if hasher:
            received_md5 = hasher.digest()
            if metadata['content-md5'] != received_md5:
                raise BadRequest(
                    'Received content MD5 %r does not match expected %r.' 
                    % (received_md5, metadata['content-md5'])
                    #% (binascii.hexlify(received_md5), binascii.hexlify(metadata['content-md5'].encode()))
                )

        return "test"
               
    def get_content(self, name, version, metadata={}, aux={}):
        return self.get_content_range(name, version, metadata, aux=aux)
     
    def get_content_range(self, name, version, metadata={}, get_slice=None, aux={}):
        """Return (nbytes, metadata, data_iterator) tuple for existing file-version object."""
        dirname, relname = self._dirname_relname(name, version)
        fullname = "%s/%s" % (dirname, relname)
        nbytes = os.path.getsize(fullname)

        if get_slice is not None:
            pos = coalesce(get_slice.start, 0)
            limit = coalesce(get_slice.stop, nbytes)
        else:
            pos = 0
            limit = nbytes

        if pos != 0 or limit != nbytes:
            # most object metadata does not apply to partial read content
            metadata = {
                k: v
                for k, v in metadata.items()
                if k in {'content-type'}
            }
            
        length = limit - pos

        def helper():
            if 'content-md5' in metadata:
                hasher = hashlib.md5()
            else:
                hasher = None

            rpos = pos
            eof = False
            with open(fullname, 'rb') as f:
                f.seek(rpos)
                while not eof:
                    buf = f.read(min(limit-rpos, self._bufsize))
                    buflen = len(buf)
                    rpos += buflen
                    
                    if hasher:
                        hasher.update(buf)

                    if rpos >= (limit-1):
                        eof = True
                    elif buflen == 0:
                        raise IOError('Read truncated at %s when %s expected.' % (rpos, limit))

                    if eof and hasher:
                        retrieved_md5 = hasher.digest()
                        if metadata['content-md5'] != retrieved_md5:
                            raise IOError(
                                'Retrieved content MD5 %s does not match expected %s.'
                                % (binascii.hexlify(retrieved_md5), binascii.hexlify(metadata['content-md5']))
                            )

                    yield buf

        return (length, metadata, helper())

    def delete(self, name, version, aux={}):
        """Delete object version."""
        dirname, relname = self._dirname_relname(name, version)
        fullname = "%s/%s" % (dirname, relname)
        os.remove(fullname)

    def delete_namespace(self, name):
        """Tidy up after an empty namespace that has been deleted."""
        dirname, relname = self._dirname_relname(name, 'dummy')
        try:
            os.removedirs(dirname)
        except OSError:
            pass
