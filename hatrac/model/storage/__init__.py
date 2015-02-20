
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Object bulk storage for Hatrac.

"""

from hatrac.core import config

backend = config.get('storage_backend')

if backend == 'filesystem':
    from filesystem import HatracStorage
elif backend == 's3':
    from s3 import HatracStorage
else:
    HatracStorage = None


