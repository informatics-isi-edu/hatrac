"""Input stream file-like wrapper for uploading data to S3. 

This module wraps mod_wsgi_input providing implementations of
seek and tell that are used by boto (but not relied upon)

"""
import logging

logger = logging.getLogger('hatrac')

class InputWrapper():
    # We can't extend mod_wsgi.Input

    def __init__(self, ip):
        self._mod_wsgi_input = ip

    def read(self, size=None):
        return self._mod_wsgi_input.read(size)

    def tell(self):
        return 0

    def seek(self, offset, whence=None):
        return 0      

    def name(self):
        return self._mod.wsgi_input.name()
