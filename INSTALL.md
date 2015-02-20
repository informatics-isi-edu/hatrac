# Hatrac Installation

[Hatrac](http://github.com/informatics-isi-edu/hatrac) (pronounced
"hat rack") is a simple object storage service for web-based,
data-oriented collaboration.

## Summary

1. Install prerequisites
  - Apache HTTPD
  - PostgreSQL
  - Python
  - web.py
  - webauthn2
1. Install hatrac Python package
  - `python setup.py install`
1. Create `hatrac` daemon account
1. Create `hatrac` PostgreSQL role
1. Create and initialize `hatrac` database
1. Create file storage directory under Apache
1. Configure `~hatrac/hatrac_config.json`
1. Configure `~hatrac/webauthn2_config.json`
1. Configure `mod_wsgi` to run Hatrac

## Basic testing

You can perform some local testing of Hatrac without configuring the
whole web service stack:

    # make sure Hatrac is installed
    % python setup.py install
    
    # get rid of any previous test data
    % dropdb hatrac_test
    % rm -rf hatrac_test_data

    # create empty test database
    % createdb hatrac_test
    
    # run tests
    % python test/smoketest.py

The test script should run to completion without printing anything. If
it encounters errors, diagnostics will be printed and the script will
exit.  You MUST start with an empty test database and empty test data
directory prior to each run of the test.

## Example hatrac_config.json

This configuration works on a Fedora host:

    {
        "storage_backend": "filesystem",
        "storage_path": "/var/www/hatrac",
        "database_type": "postgres",
        "database_name": "",
        "database_schema": "hatrac",
        "database_max_retries": 5
    }

## REST API testing

You can perform system testing of the whole web service stack, if
configured with cookie-based authentication (such as using a companion
ERMrest installation as described in the subsequent configration
examples of this document):

    # manually create a session cookie
    % curl -b cookie -c cookie \
       -d username=testuser -d password=testpassword \
       https://$(hostname)/ermrest/auth/session

    # run the test script
    % COOKIES=cookie bash test/rest-smoketest.sh

## Example Webauthn2 configuration 

This configuration allows Hatrac to share an existing Webauthn2
deployment from an ERMrest installation (if ERMrest is configured with
the same cookie name and path settings):

    {
          "require_client": true,
          "require_attributes": true, 
          "listusers_permit": ["admin"], 
          "listattributes_permit": ["admin"], 
          "manageusers_permit": ["admin"], 
          "manageattributes_permit": ["admin"], 
                
          "session_expiration_minutes": 30, 
          "def_passwd_len": 10, 
          "hash_passwd_reps": 1000,
            
          "sessionids_provider": "webcookie", 
          "sessionstates_provider": "database", 
          "clients_provider": "database", 
          "attributes_provider": "database", 
            
          "handler_uri_usersession": "/ermrest/authn/session", 
            
          "web_cookie_name": "ermrest", 
          "web_cookie_path": "/", 
          "web_cookie_secure": true, 
          "setheader": false,
    
          "database_schema": "webauthn2", 
          "database_type": "postgres", 
          "database_name": "ermrest", 
          "database_max_retries": 5
    }

**Note**: at present, Hatrac does not expose any Webauthn2 REST APIs
so you MUST share an existing deployment as above if you want to use a
local account and session-based login for testing.  Additionally, you
must then grant the `ermrest` role to the `hatrac` role in PostgreSQL
so that Hatrac can look up client authentication information at
runtime.

## Example Apache configuration

See the sample `wsgi_hatrac.conf` file in the git repo, which would be
installed under `/etc/httpd/conf.d/` on Red Hat flavored machines:

    AllowEncodedSlashes On
    
    WSGIPythonOptimize 1
    WSGIDaemonProcess hatrac processes=4 threads=4 user=hatrac maximum-requests=2000
    WSGIScriptAlias /hatrac /usr/lib/python2.7/site-packages/hatrac/hatrac.wsgi
    WSGIPassAuthorization On
    
    WSGISocketPrefix /var/run/wsgi/wsgi
    
    <Location /hatrac>
    
       Require all granted
    
       WSGIProcessGroup hatrac
        
    </Location>

## Working with SE-Linux

The following is an example set of commands to allow Hatrac to write
to the filesystem in a Fedora installation.  On other distributions,
the appropriate path and SE-Linux contexts might vary slightly:

    setsebool -P httpd_can_network_connect_db on
    semanage fcontext --add --type httpd_sys_rw_content_t "/var/www/hatrac(/.*)?"
    restorecon -rv /var/www/hatrac

