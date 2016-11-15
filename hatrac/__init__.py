
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import sys
import core
import model
import rest
import psycopg2

def instantiate(config):
    """Return a directory service instance for config."""
    # instantiate storage singleton
    storage = model.HatracStorage(config)
    # instantiate directory singleton
    directory = model.HatracDirectory(config, storage)
    return directory

# instantiate a default singleton
try:
    directory = instantiate(core.config)
except psycopg2.OperationalError:
    directory = None

# TODO: conditionalize this if we ever have alternate directory impls
def deploy_cli(argv, config=None):
    """Deploy initial Hatrac DB content and ACLs.

       If config is not None, use it to instantiate a custom service,
       otherwise use the external configuration file.
    """
    if config is not None:
        deploy_dir = instantiate(config)
    else:
        deploy_dir = directory
    if len(argv) > 1:
        root_roles = argv[1:]
        deploy_dir.deploy_db(root_roles)
        deploy_dir.schema_upgrade()
        return 0
    else:
        sys.stderr.write("""
Usage: %(cmd)s role...

With a preconfigured ~/hatrac_config.json, this command will populate
the required database tables for an empty service.

The command-line arguments are interpreted as a list of administrative
roles to be given joint ownership of the root namespace such that
subsequent administration can be performed via the REST API.

The supplied role names must exactly match role names that will be
determined by the webauthn2 client and attribute providers configured
for the deployment.
""" % dict(
    cmd=argv[0]
)
        )
        return 1

