
#
# Copyright 2015-2019 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

import sys
import psycopg2
import os

from . import core
from . import model

def instantiate(config):
    """Return a directory service instance for config."""
    # instantiate storage singleton
    if model.HatracStorage is None:
        raise core.HatracConfigError('storage backend not configured')

    storage = model.HatracStorage(config)

    # instantiate directory singleton
    try:
        return model.HatracDirectory(config, storage)
    except psycopg2.OperationalError as e:
        raise core.HatracConfigError('metadata directory DB not accessible: %s' % e)

# instantiate a default singleton
try:
    directory = instantiate(core.config)
except core.HatracConfigError as e:
    sys.stderr.write('WARNING: could not instantiate hatrac backend singletons: %s\n' % e)
    directory = None

try:
    from . import rest
except psycopg2.OperationalError as e:
    sys.stderr.write('WARNING: could not import hatrac rest service module: %s\n' % e)
    pass

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

def sample_httpd_config():
    """Emit sample wsgi_hatrac.conf to standard output."""
    path = __path__[0]
    if path[0] != '/':
        path = '%s/%s' % (
            os.path.dirname(loader.get_filename('hatrac')),
            path
        )
    sys.stdout.write("""
# this file must be loaded (alphabetically) after wsgi.conf
AllowEncodedSlashes On

WSGIPythonOptimize 1
WSGIDaemonProcess hatrac processes=4 threads=4 user=hatrac maximum-requests=2000
WSGIScriptAlias /hatrac %(hatrac_location)s/hatrac.wsgi process-group=hatrac
WSGIPassAuthorization On

WSGISocketPrefix /var/run/httpd/wsgi

<Location /hatrac>

   AuthType none
   Require all granted
   #AuthType webauthn
   #Require webauthn-optional

   WSGIProcessGroup hatrac
    
   # site can disable redundant service logging by adding env=!dontlog to their CustomLog or similar directives
   SetEnv dontlog

</Location>
""" % {
    'hatrac_location': path,
}
    )
