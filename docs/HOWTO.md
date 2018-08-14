
# How-tos

This document assumes a basic familiarity with:

  - HTTP protocol and web concepts
  - Object storage concepts
  - Command-line environments and the `curl` utility

See the [REST API documentation](REST-API.md) for a complete overview
of the supported resources and operations with detailed HTTP protocol
information.

## Background

Hatrac provides a client-controlled namespace hierarchy with
client-named objects containing arbitrary data content. In short, a
Hatrac namespace is similar to a directory in a traditional
filesystem, while a Hatrac object is similar to a file.

However, a single Hatrac object name may have multiple content
versions. Each version is given a unique, psuedo-random version
identifier when it is created. Any particular object version is
immutable and can only ever be seen with the same content that was
provided atomically when it was created. The bare object name
implicitly references the _latest_ available version, while
version-qualified names reference the particular version no matter
what other updates are performed on the object.

## Examples

In the following examples, let's assume that Hatrac is deployed with a
webauthn2 configuration that uses local database
providers. Furthermore, assume that a test user `test-user-1` is
defined and has been assigned an attribute `my-admin-group`. For this
deployment, assume the hostname `server.example.org` is hosting the
service stack.

### Deploying 

This command initializes the deployment so that users with the
`my-admin-group` attribute are granted full privileges on the root
namespace:

    hatrac-deploy "my-admin-group"

### Authentication using Database Providers

Hatrac currently depends on a sibling ERMrest deployment to have
access to its embedded webauthn2 security service for login. With the
webauthn2 `database` providers, we can login via the command-line by
sending an HTTP POST request with login form fields populated:

    curl -b ~/cookie -c ~/cookie \
      -d username=test-user-1 \
      -d password="my secret" \
      https://server.example.org/ermrest/authn/session

This command prints out the new session information as a JSON response
(with modified whitespace here for readability):

    {
      "attributes": [
        {"display_name": "my-admin-group", "id": "my-admin-group"}, 
        {"display_name": "test-user-1", "id": "test-user-1"}
      ], 
      "seconds_remaining": 1799, 
      "since": "2016-05-06 23:38:15.798275+00:00", 
      "expires": "2016-05-07 00:08:15.798275+00:00", 
      "client": {"display_name": "test-user-1", "id": "test-user-1"}
    }

With other security providers, more effort is required to obtain the
equivalent `~/cookie` file needed for the rest of the examples below.

### Listing the Root Namespace

The root namespace is listed with an HTTP GET on the namespace URL:

    curl -b ~/cookie -c ~/cookie \
      https://server.example.org/hatrac/

The response represents the currently empty root namespace:

    []

### Creating a Nested Namespace

The HTTP PUT operation with a special `Content-Type` header allows us
to create a new namespace with our own chosen name:

    curl -b ~/cookie -c ~/cookie \
      -H "Content-Type: application/x-hatrac-namespace" \
      -X PUT
      https://server.example.org/hatrac/folder1

The response repeats the name we have chosen for the new namespace:

    /hatrac/folder1

If you repeat the namespace listing step for the root namespace, this
time the response will enumerate the single child namespace:

    ["/hatrac/folder1"]

### Creating an Object

First, let's create a test data file:
    
    cat > hello.txt <<EOF
    > Line 1: Hello, World!
    > Line 2: That's all, Folks!
    EOF

Now, let's upload it using the built-in support for file upload via
HTTP PUT using curl's `-T` transfer mode:

    curl -b ~/cookie -c ~/cookie \
      -H "Content-Type: text/plain" \
      -T hello.txt \
      https://server.example.org/hatrac/folder1/object1

The response repeats the object name we have chosen but qualifies it
with a server-generated _version ID_:

    /hatrac/folder1/object1:SP274AQOOO3TOXIS2BVSDA5HCE

### Retrieving an Object

A simple HTTP GET will retrieve an existing object's *current version*:

    curl -b ~/cookie -c ~/cookie \
      https://server.example.org/hatrac/folder1/object1

The response contains the object content itself:

    Line 1: Hello, World!
    Line 2: That's all, Folks!

#### Retrieving Object Metadata Too

We can also ask `curl` to dump the protocol headers with the response
on standard output using the `-D` flag:

    curl -b ~/cookie -c ~/cookie \
      -D - \
      https://server.example.org/hatrac/folder1/object1

The response contains the object content itself:

    HTTP/1.1 200 OK
    Date: Fri, 06 May 2016 23:55:28 GMT
    Content-Length: 49
    Content-Disposition: filename*=UTF-8''object1
    ETag: "SP274AQOOO3TOXIS2BVSDA5HCE"
    Vary: cookie
    Content-Type: text/plain; charset=UTF-8
    
    Line 1: Hello, World!
    Line 2: That's all, Folks!

The `ETag` header is used to inform the HTTP client about cache
validity. It happens to repeat the actual version ID but you SHOULD
NOT rely on this in any client application. The Hatrac service MAY use
other means for cache management in future revisions.

### Updating an Object

First, let's modify our existing data file:

    cat >> hello.txt <<EOF
    > Line 3: Well, I spoke too soon...
    > EOF

Now, let's upload it the same as before. The server will automatically
create a new version and update the *current version* to point to this
new one:

    curl -b ~/cookie -c ~/cookie \
      -H "Content-Type: text/plain" \
      -T hello.txt \
      https://server.example.org/hatrac/folder1/object1

The response repeats the object name we have chosen but qualifies it
with a server-generated _version ID_:

    /hatrac/folder1/object1:3VJO6XIPAGVBAGPUIOMG546SWU

If you repeat the step to retrieve object and metadata, you will see
the new content:

    HTTP/1.1 200 OK
    Date: Sat, 07 May 2016 00:00:05 GMT
    Content-Length: 83
    Content-Disposition: filename*=UTF-8''object1
    ETag: "3VJO6XIPAGVBAGPUIOMG546SWU"
    Vary: cookie
    Content-Type: text/plain; charset=UTF-8
    
    Line 1: Hello, World!
    Line 2: That's all, Folks!
    Line 3: Well, I spoke too soon...

### Retrieving an Object Version

Simply by using the full version-qualified URL for the object, you can
retrieve the older version of the object:

    curl -b ~/cookie -c ~/cookie \
      -D - \
      https://server.example.org/hatrac/folder1/object1:SP274AQOOO3TOXIS2BVSDA5HCE

The response will familiar:

    HTTP/1.1 200 OK
    Date: Sat, 07 May 2016 00:04:55 GMT
    Content-Length: 49
    Content-Disposition: filename*=UTF-8''object1
    ETag: "SP274AQOOO3TOXIS2BVSDA5HCE"
    Vary: cookie
    Content-Type: text/plain; charset=UTF-8
    
    Line 1: Hello, World!
    Line 2: That's all, Folks!

### Deleting an Object Version

You can delete a specific object version and it will no longer be
available for retrieval.  If you delete the *current version*, the
object will automatically revert to the _latest_ remaining version as
its new current version:

    curl -b ~/cookie -c ~/cookie \
      -X DELETE \
      https://server.example.org/hatrac/folder1/object1:3VJO6XIPAGVBAGPUIOMG546SWU

This operation has no response content.  If you now retrieve the
unqualified object name, you'll get the previous version again.

### Deleting an Object

You can delete the entire object and all of its versions at once:

    curl -b ~/cookie -c ~/cookie \
      -X DELETE \
      https://server.example.org/hatrac/folder1/object1

This operation has no response content.  If you now retrieve the
unqualified object name, you'll get a `404 Not Found` HTTP error.
