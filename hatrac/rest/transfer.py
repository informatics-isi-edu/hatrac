
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from core import web_url


@web_url([
    # path, name, job, chunk
    '/([^/:;]+/)*([^/:;]+);upload/([^/:;]+)/([^/:;]+)'
])
class ObjectTransferChunk (object):

    def __init__(self):
        pass

    def PUT(self, path, name, job, chunk):
        """Upload chunk of transfer job."""
        pass


@web_url([
    # path, name, job
    '/([^/:;]+/)*([^/:;]+);upload/([^/:;]+)/?'
])
class ObjectTransfer (object):

    def __init__(self):
        pass

    def PUT(self, path, name, job):
        """Update status of transfer job to finalize."""
        pass

    def DELETE(self, path, name, job):
        """Cancel existing transfer job."""
        pass

    def GET(self, path, name, job):
        """Get status of transfer job."""
        pass


@web_url([
    # path, name
    '/([^/:;]+/)*([^/:;]+);upload/?'
])
class ObjectTransfers (object):

    def __init__(self):
        pass

    def POST(self, path, name):
        """Create a new chunked transfer job."""
        pass

    def GET(self, path, name):
        """List outstanding chunked transfer jobs."""
        pass
        

