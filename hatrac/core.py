
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Core module config

"""

from webauthn2 import merge_config

config = merge_config(
    jsonFileName='hatrac_config.json'
)

class HatracException (Exception):
    """Base class for Hatrac API exceptions."""
    pass

class BadRequest (HatracException):
    """Exceptions representing malformed requests."""
    pass

class Conflict (HatracException):
    """Exceptions representing conflict between usage and current state."""
    pass

class Forbidden (HatracException):
    """Exceptions representing lack of authorization for known client."""
    pass

class Unauthenticated (HatracException):
    """Exceptions representing lack of authorization for anonymous client."""
    pass

class NotFound (HatracException):
    """Exceptions representing attempted access to unknown resource."""
    pass

