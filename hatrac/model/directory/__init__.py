
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Stateful namespace directory.

A single rdbms implementation is statically configured.

"""

# TODO: implement runtime configuration if alternative directory
# implementations are ever added to this project

from pgsql import HatracDirectory

