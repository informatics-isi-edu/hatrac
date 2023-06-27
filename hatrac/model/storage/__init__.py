
#
# Copyright 2015-2023 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Object bulk storage for Hatrac.

"""

from ...core import config

backend = config.get('storage_backend')

if backend == 'filesystem':
    from .filesystem import HatracStorage
elif backend == 'amazons3':
    from .amazons3 import HatracStorage
elif backend == 'overlay':
    from .overlay import HatracStorage
else:
    HatracStorage = None


