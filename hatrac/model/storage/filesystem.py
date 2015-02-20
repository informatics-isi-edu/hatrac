
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

            if nbytes is not None:
                if rbytes >= nbytes:
                    eof = True
                elif buflen == 0:
                    f.close()
                    raise IOError('Only received %s of %s expected bytes.' % (rbytes, nbytes))
            elif buflen == 0:
                eof = True

        return version

    def get_content(self, name, version):
        """Return (nbytes, content_type, content_md5, data_iterator) tuple for existing file-version object."""
        dirname, relname = self._dirname_relname(name, version)
        fullname = "%s/%s" % (dirname, relname)
        nbytes = os.path.getsize(fullname)
        
        def helper():
            rbytes = 0
            eof = False
            with open(fullname, 'rb') as f:
                while not eof:
                    buf = f.read(min(nbytes-rbytes, self._bufsize))
                    buflen = len(buf)
                    rbytes += buflen
                    
                    yield buf

                    if rbytes >= nbytes:
                        eof = True
                    elif buflen == 0:
                        raise IOError('Only read %s of %s expected bytes.' % (rbytes, nbytes))

        return (nbytes, None, None, helper())

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
