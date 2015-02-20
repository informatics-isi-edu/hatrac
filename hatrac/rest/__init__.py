
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import core

# these modify core.dispatch_rules
import acl
import name
import transfer

urls = list(core.dispatch_rules.items())

# sort longest patterns first where prefixes match
urls.sort(reverse=True)

# flatten list of pairs into one long tuple for web.py
urls = tuple(
    [ item for rule in urls for item in rule ]
)

