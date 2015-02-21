
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Filesystem-backed object bulk storage for Hatrac.

This module handles only low-level byte storage. Object and
object-version lifecycle and authorization is handled by the caller.

"""
import os
import hashlib
import base64
import random
import struct
from StringIO import StringIO

from hatrac.core import BadRequest

def make_file(dirname, relname, accessmode):
    """Create and open file with accessmode, including missing parents.

    Returns fp.

    """
    # TODO: test for conflicts during creation?
    filename = "%s/%s" % (dirname, relname)

    if not os.path.exists(dirname):
        os.makedirs(dirname, mode=0755)

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

    def create_from_file(self, name, input, nbytes, content_type=None, content_md5=None):
        """Create an entire file-version object from input content, returning version ID."""
        
        version = base64.b32encode( 
            (struct.pack('Q', random.getrandbits(64))
             + struct.pack('Q', random.getrandbits(64)))[0:26]
        ).replace('=', '') # strip off '=' padding

        dirname, relname = self._dirname_relname(name, version)
        f = make_file(dirname, relname, 'wb')

        # upload whole content at offset 0 (for code reuse)
        self.upload_chunk_from_file(None, None, 0, 0, input, nbytes, content_md5, f)
        return version

    def create_upload(self, name, nbytes=None, content_type=None, content_md5=None):
        version = self.create_from_file(name, StringIO(''), 0)
        return version

    def upload_chunk_from_file(self, name, version, position, chunksize, input, nbytes, content_md5=None, f=None):
        if f is None:
            dirname, relname = self._dirname_relname(name, version)
            fullname = "%s/%s" % (dirname, relname)
            
            f = open(fullname, "r+b")
        f.seek(position*chunksize)

        if content_md5:
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
                elif buflen == 0:
                    f.close()
                    raise BadRequest('Only received %s of %s expected bytes.' % (rbytes, nbytes))
            elif buflen == 0:
                eof = True

        if hasher:
            received_md5 = hasher.hexdigest().lower()
            if content_md5.lower() != received_md5:
                raise BadRequest(
                    'Received content MD5 %s does not match expected %s.' 
                    % (received_md5, content_md5)
                )
                    
    def get_content(self, name, version, content_md5=None):
        """Return (nbytes, content_type, content_md5, data_iterator) tuple for existing file-version object."""
        dirname, relname = self._dirname_relname(name, version)
        fullname = "%s/%s" % (dirname, relname)
        nbytes = os.path.getsize(fullname)
        
        def helper():
            if content_md5:
                hasher = hashlib.md5()
            else:
                hasher = None

            rbytes = 0
            eof = False
            with open(fullname, 'rb') as f:
                while not eof:
                    buf = f.read(min(nbytes-rbytes, self._bufsize))
                    buflen = len(buf)
                    rbytes += buflen
                    
                    if hasher:
                        hasher.update(buf)

                    if rbytes >= nbytes:
                        eof = True
                    elif buflen == 0:
                        raise IOError('Only read %s of %s expected bytes.' % (rbytes, nbytes))

                    if eof and hasher:
                        retrieved_md5 = hasher.hexdigest().lower()
                        if content_md5.lower() != retrieved_md5:
                            raise IOError(
                                'Retrieved content MD5 %s does not match expected %s.'
                                % (retrieved_md5, content_md5)
                            )

                    yield buf

        return (nbytes, None, content_md5, helper())

    def delete(self, name, version):
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
