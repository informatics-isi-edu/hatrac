# REST API

[Hatrac](http://github.com/informatics-isi-edu/hatrac) (pronounced
"hat rack") is a simple object storage service for web-based,
data-oriented collaboration. It presents a simple HTTP REST API with
the following characteristics:

## Contents

### Main Topics

This documentation is broken down into the following general topics:

1. [URL Conventions](#url-conventions)
2. [Resource Types Overview](#resource-naming-and-lifecycle-rules)
3. [Root Namespace](#root-namespace-resource)
4. [Nested Namespaces](#nested-namespace-resources)
5. [Objects](#object-resources)
6. [Object Versions](#object-version-resources)
7. [Metadata](#metadata-sub-resources)
8. [Access Control Lists](#access-control-list-sub-resources)
9. [Chunked Uploads](#chunked-upload-resources)

### Quick Links to Operations

The REST API supports the following operations.

1. Namespace operations
  - [Get namespace listing](#namespace-listing-retrieval)
  - [Create nested namespace](#nested-namespace-creation)
  - [Delete nested namespace](#nested-namespace-deletion)
2. Object operations
  - [Create or update object](#object-creation-and-update)
  - [Get object content](#object-retrieval)
  - [Delete object](#object-deletion)
3. Object version operations
  - [Get object version list](#object-version-list-retrieval)
  - [Create object version](#object-creation-and-update)
  - [Get object version content](#object-version-retrieval)
  - [Delete object version](#object-version-deletion)
4. Metadata management operations
  - [Get metadata collection](#metadata-collection-retrieval)
  - [Get metadata value](#metadata-value-retrieval)
  - [Create or update metadata value](#metadata-value-creation-and-update)
  - [Delete metadata value](#metadata-value-deletion)
5. Access control list operations
  - [Get access controls](#access-control-retrieval)
  - [Update access control list](#access-control-list-update)
  - [Clear access control list](#access-control-list-deletion)
6. Chunked upload operations
  - [Get upload job listing](#chunked-upload-job-listing-retrieval)
  - [Create upload job](#chunked-upload-job-creation)
  - [Get upload job status](#chunked-upload-job-status-retrieval)
  - [Upload data chunk](#chunk-upload)
  - [Finalize upload job](#chunked-upload-job-finalization)
  - [Cancel upload job](#chunked-upload-job-cancellation)

## URL Conventions

Any Hatrac URL is a valid HTTP URL and contains user-generated content
which may need to be escaped. Several reserved characters from RFC
3986 are used as meta-syntax in Hatrac and MUST be escaped if they are
meant to be part of a user-generated name value and MUST NOT be
escaped if they are meant to indicate the Hatrac meta-syntax:
- The '/' or forward-slash, used as a path separator character
- The ':' or colon, used to separate object names from version IDs
- The ';' or semi-colon, used to separate resource names and sub-resource keywords

All other reserved characters should be escaped in user-generated
content in URLs, but have no special meaning to Hatrac when appearing
in unescaped form.

## Resource Naming and Lifecycle Rules

The hierarchical Hatrac naming model defines three main types of resource:

1. Namespace
1. Object
1. Object Version

The model supports a hierarchy of nested namespaces while objects and
their versions appear only at the leaves of the tree. A particular
hierarchical name has a three-phase lifecycle that can oscillate in
the latter two phases:

1. Undefined, until the name is bound
1. Defined, from the moment the name is bound until deleted
  - Namespaces: a name bound as a namespace will always be a namespace
    if available to access.
  - Objects: a name bound as an object will always be an object if
    available to access.  
1. Deleted, from the moment the name is deleted until restored to its
   previous definition.
    
An implementation MAY permit _restoration_ of a deleted namespace or
object, but it is not required to do so. An implementation MUST
prevent other reuse of names.  The definition of restoration is that
all of the following hold:
- The name is defined as the same type of resource that was previous
  defined prior to deleted;
- At the moment of restoration, the parent namespace encoded in the
  name is still a defined namespace;
- When an object name is restored, the constraints on object version
  naming continue to hold as if the object had never been deleted;
- When a namespace is restored, any child namespace or object MAY be
  restored and MAY remain deleted. Any child restoration MUST follow
  the same restoration rules, recursively.

### Object Version Naming

A particular object name can be qualified with a version identifier
having a three-phase lifecycle that can oscillate on the latter two
phases:

1. Undefined, until the version identifier is issued to a content
value
1. Defined, from the moment a version is created until the version is
deleted
1. Unavailable, from the moment a version is deleted until it is
restored with the same content value

Hatrac allows object versions to be deleted in order to reclaim
backing storage resources, as an alternative to simply making versions
unavailable by restricting their access control settings.

### Referential Stability

A particular namespace or object name denotes the same abstract
container from the point of definition into the indefinite future, but
that container can change.  Namespaces can gain or lose children
(nested namespaces and objects) and objects can gain or lose object
versions.

A particular object version reference (name plus version identifier)
is a permanent, immutable reference to a specific content value. Such
a referenced content value MAY become unavailable if the object
version is deleted or its access control rules are
restrictive. However, when and if it becomes available, it MUST always
denote the same content value for all clients able to access it at any
point in time. To guarantee this stability for clients while giving
some freedom to implementers, the following rules are defined for
version identifiers:

- Version identifiers are opaque, local qualifiers for a specific
  object name. An implementation MAY use globally distinct version
  identifiers but clients SHOULD NOT compare version identifiers
  associated with different object names.
- Any two distinct content values applied as updates to the same
  object MUST be issued distinct version identifiers.  Hence, any two
  reference URLs with identical object name and version identifier
  MUST denote the same content value.
- Any two updates applied to the same object with identical content
  value are subject to more complex rules depending on the sequencing
  of operations:
  1. If the first update yields an object version which is deleted
  prior to the second update operation
    - The implementation MAY reuse the same version identifier used
      for the previously deleted object version which denoted the same
      content value.
    - The implementation MAY issue a distinct version identifier for
      each object version that has non-overlapping lifetimes while
      denoting the same content value.
  1. If the first update yields an object version which is still
  defined prior to the second update operation
    - The second operation MAY fail with a conflict if the
      implementation does not support storage and tracking of
      duplicate content values
    - The second operation MAY issue a new, distinct version
      identifier
  1. Simultaneous update must be logically resolved as if one of the
     updates occurred before the other, satisfying the preceding
     rules.
      
These rules allow a broad range of implementation techniques while
preventing collaboration hazards such as unstable references denoting
different data values at different times or data value collisions
causing ambiguous object ownership and privileges.
  

## Root Namespace Resource

The root of a Hatrac deployment is an HTTPS URL of the form:

- https:// _authority_ / _prefix_ 

Where _authority_ is a DNS hostname and optional port number, and
_prefix_ is a '/' separated string of any length.  A deployment MAY
use a fixed path to route HTTP requests to Hatrac alongside other
services in the same authority (host and port), or it MAY use an empty
prefix if the entire HTTP namespace of the authority is dedicated to
Hatrac resources.

In all documentation below, the "/ _prefix_" is considered to be part
of the _parent path_ URL path elements.  Therefore every example URL
will be a hierarchical name starting with a "/" character.

### Namespace Listing Retrieval

The GET operation is used to list direct children of a namespace:

    GET /parent_path/namespace_id
    Host: authority_name
    If-None-Match: etag_value

for which a successful response is a JSON array of child resource
URLs:

    200 OK
    Content-Type: application/json
    Content-Length: N
    ETag: etag_value
    
    ["/hatrac/parent_path/namespace_id/child1", ...]
    
If the `text/uri-list` content-type is negotiated, the response will
be a whitespace separated list of child URLs:

    200 OK
	Content-Type: text/uri-list
	Content-Length: N
	ETag: etag_value
	
	/hatrac/parent_path/namespace_id/child1
	/hatrac/parent_path/namespace_id/child2
	...

### Namespace Listing Metadata Retrieval

The HEAD operation is used to get basic status information:

    HEAD /parent_path/namespace_id
    Host: authority_name

for which a successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N

indicating that an `N` byte JSON representation is available.  This
operation is essentially equivalent to the GET operation but with the
actual child listing elided.

### Deletion of Root Namespace Forbidden

The root namespace of a Hatrac deployment SHOULD forbid delete
operations. It is nonsensical to have a Hatrac deployment without a
root namespace.

## Nested Namespace Resources

Any hierarchical namespace in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _parent path_ / _namespace id_

Where _parent path_ is the name of the enclosing namespace and
_namespace id_ is the relative name of the nested namespace. Of
course, the enclosing namespace may be the root namespace of the
deployment, e.g. `/hatrac`, or another nested namespace,
e.g. `/hatrac/some/ancestors`.

### Nested Namespace Creation

The PUT operation is used to create a new nested namespace:

    PUT /parent_path/namespace_id
    Host: authority_name
    Content-Type: application/x-hatrac-namespace

which may also be modified with the `parents` query parameter:

    PUT /parent_path/namespace_id?parents=true
    Host: authority_name
    Content-Type: application/x-hatrac-namespace

to request automatic creation of missing ancestors in _parent path_.
In either case, a successful response is:

    201 Created
    Location: /parent_path/namespace_id
    Content-Type: text/uri-list
    Content-Length: N

    /parent_path/namespace_id

**Note**: see related object resource interface for pragmatic
discussion of the use of Content-Type to disambiguate namespace and
object creation requests.

Typical PUT error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous creation of such a namespace is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to create or update the namespace.
- **404 Not Found**: the parent namespace does not exist and
  `parents=true` query parameter was not supplied to request automatic
  creation of missing ancestors.
- **409 Conflict**: the namespace cannot be created due to a
  conflict with existing state of the service:
  - The _parent path_ does not denote a namespace
  - The namespace already exists

### Nested Namespace Listing Retrieval

The same GET and HEAD operations documented above for the Root
Namespace Resource can also list direct children of any nested
namespace.

For nested namespaces, typical GET or HEAD error responses would be:
- **404 Not Found**: the name does not map to an available resource on
  the server.

**Note**: since nested namespaces and objects share the same
hierarchical name structure, a GET operation on a name might resolve
to an object rather a namespace. As such, error responses applicable
to an object might be encountered as well.

### Nested Namespace Deletion

The DELETE operation is used to delete a nested namespace

    DELETE /parent_path/namespace_id
    Host: authority_name

for which a successful response is:

    204 No Content
    
An implementation SHOULD NOT allow deletion of non-empty
namespaces. It is RECOMMENDED that deletion of non-empty namespaces be
rejected, but an implementation MAY treat it as a bulk request by the
same client to delete everything contained in the namespace prior to
deleting the namespace itself.  It is further RECOMMENDED that such a
deletion be processed atomically, considering all client privileges in
advance, but an implementation MAY partially delete contents before
failing due to an authorization error on some subset of contents.

Typical DELETE error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous deletion of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to delete the resource.
- **404 Not Found**: the name does not denote an existing resource.
- **409 Conflict**: the resource cannot be deleted at this time,
    i.e. because the namespace is not empty.

## Object Resources

Any unversioned object name in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _namespace path_ / _object name_
  
Where _namespace path_ is the name of the enclosing namespace and
_object name_ is the relative name of the object.

### Object Creation and Update

The PUT operation is used to create a new object or a new version of
an existing object.  Literal object content is provided as input:

    PUT /namespace_path/object_name
    Host: authority_name
    Content-Type: text/plain
    Content-Length: 14
    Content-MD5: ZXS/CYPMeEBJpBYNGYhyjA==
	Content-SHA256: 5+aEMqzlEZxe9xPaDUZ0GyBvTUaZf4s0yMpPgV/0yt0=
	Content-Disposition: filename*=UTF-8''test.txt
	If-Match: etag_value
	If-None-Match: *
    
    ...content...

This example has metadata consistent with an object containing a
single Unix-style text line `...content...\n` inclusive of the
line-terminator.

As with nested namespace creation, an optional query parameter may be
included to enable automatic namespace creation:

    PUT /namespace_path/object_name?parents=true
	...

The optional `If-Match` and `If-None-Match` headers MAY be specified
to limit object update to specific scenarios. In a normal situation,
only one of these two headers is specified in a single request:
  - An _etag value_ with the `If-Match` header requires that the current version of the object on the server match the version indicated by the _etag value_ in order for the object to be updated as per the request _content_.
  - An `*` with the `If-None-Match` header requires that the object lack a current version on the server in order for the object to be created or updated as per the request _content_.

Without either `If-Match` or `If-None-Match` headers in the request,
the update will be unconditionally applied if allowed by policy and
the current state of the server. When supplied, they select HTTP
standard conditional request processing.

The optional `Content-MD5` and `Content-SHA256` headers can carry an
MD5 or SHA-256 _hash value_, respectively. The _hash value_ SHOULD be
the base64 encoded representation of the underlying bit sequence
defined by the relevant hash algorithm standard. Either or both, if
supplied, will be stored and returned with data retrieval responses,
useful for end-to-end data integrity checks by clients. An
implementation MAY checksum the supplied _content_ and reject the
request if it mismatches any supplied _hash value_ or if any _hash
value_ is malformed. An implementation MAY recognize and accept
hex-encoded _hash value_ or MAY reject it as a bad request, but in
either case it MUST always return proper base64-encoded _hash value_
in any service-issued `Content-MD5` or `Content-SHA256` response
header.

The optional `Content-Disposition` header will be stored and returned
with data retrieval responses. An implementation MAY restrict which
values are acceptable as content disposition instructions. Every
implementation SHOULD support the `filename*=UTF-8''` _filename_
syntax where _filename_ is a basename with no path separator
characters. According to the web standards, the _filename_ component
embedded in this header MUST be UTF-8 which is then URL-escaped
(percent-encoded) on an octet by octet basis, just like URL components
in this REST API.

A successful response is:

    201 Created
    Location: /namespace_path/object_name:version_id
    Content-Type: text/uri-list
    Content-Length: N
    
    /namespace_path/object_name:version_id

The successful response includes the _version id_ qualified name of
the newly updated object.

Typical PUT error responses would be:
  - **400 Bad Request**: the client supplied a `Content-MD5` header
      with a _hash value_ that does not match the entity _content_
      which was recieved.
  - **401 Unauthorized**: the client is not authenticated and
      anonymous creation of such an object is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to create the object.
  - **404 Not Found**: the _parent path_ does not exist and the
    `parents=true` query parameter was not supplied to request
    automatic creation of missing ancestors.
  - **409 Conflict**: the object cannot be created due to a
      conflict with existing state of the service:
    - The _namespace path_ may not denote a namespace
    - The _object name_ may already be in use as a namespace,
      therefore preventing its use as an object.
  - **412 Precondition Failed**: the object cannot be created or updated due to its current state on the server not meeting the requirements indicated by the `If-Match` and/or `If-None-Match` request headers.

**Note**: There is ambiguity in the meaning of a URL when creating a
new object or nested namespace because they have the same syntactic
structure.  Hatrac disambiguates such requests in a decision process:

1. If the full path denotes an existing object, the PUT request MUST
   denote a request to update the content of the existing object,
   regardless of what `Content-Type` is present.
1. If `Content-Type` is `application/x-hatrac-namespace`, the PUT
   request MUST denote a request to create a new nested namespace.
1. Any other PUT request not matching the above is considered an
   object creation request.

This set of rules makes it simple to create any common object or
namespace. In the degenerate case where one wishes to create an object
with content that looks exactly like a namespace request input, the
solution is to first create an empty object (e.g. with `Content-Type:
text/plan`) and then immediately update its content with the desired
content.

### Object Retrieval

The GET operation is used to retrieve the current version of an object:

    GET /namespace_path/object_name
    Host: authority_name
    Accept: *
    If-None-Match: etag_value

The optional `If-None-Match` header MAY supply an `ETag` value
obtained from a previous retrieval operation, to inform the server
that the client already has a copy of a particular version of the
object.

for which a successful response is:

    200 OK
    Content-Type: content_type
    Content-Length: N
    Content-MD5: hash_value
	Content-SHA256: hash_value
	Content-Disposition: filename*=UTF-8''filename
	Content-Location: /namespace_path/object_name:version
    ETag: etag_value
    
    ...content...

The optional `Content-MD5`, `Content-SHA256`, and
`Content-Disposition` headers MUST be present if supplied during
object creation and MAY be present if the service computes missing
values in other cases. The `Content-Location` header SHOULD be present and
specifies the URL for the version of the object which was retrieved.

It is RECOMMENDED that a Hatrac server return an `ETag` indicating the version of the _content_ returned to the client.
    
Typical GET error responses would be:
  - **304 Not Modified**: the _etag value_ supplied in the `If-None-Match` header matches the current object version on the server.
  - **401 Unauthorized**: the client is not authenticated and
      anonymous retrieval of such an object is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to retrieve the object.
  - **404 Not Found**: the name does not denote a defined object.
  - **409 Conflict**: the object cannot be retrieved at this time,
      e.g. there are currently no object versions defined.

### Object Metadata Retrieval

The HEAD operation is used to retrieve information about the current
version of an object:

    HEAD /namespace_path/object_name
    Host: authority_name
    Accept: *
    
for which a successful response is:

    200 OK
    Accept-Ranges: bytes
    Content-Type: content_type
    Content-Length: N
    Content-MD5: hash_value
	Content-SHA256: hash_value
	Content-Disposition: filename*=UTF-8''filename
	Content-Location: /namespace_path/object_name:version

The HEAD operation is essentially equivalent to the GET operation but
with the actual object content elided.

### Object Version List Retrieval

The GET operation is used to list versions of an object:

    GET /namespace_path/object_name;versions
    Host: authority_name
    
for which a successful response is a JSON array of version resource
URLs:

    200 OK
    Content-Type: application/json
    Content-Length: N
    
    ["/hatrac/namespace_path/object_name:version_id", ...]
    
If the `text/uri-list` content-type is negotiated, the response
is a whitespace separated list of version URLs:

    200 OK
    Content-Type: text/uri-list
    Content-Length: N
    
    /hatrac/namespace_path/object_name:version1_id
    /hatrac/namespace_path/object_name:version2_id
    ...

### Object Deletion

The DELETE operation is used to delete an object

    DELETE /namespace_path/object_name
    Host: authority_name
    If-Match: etag_value

The optional `If-Match` header MAY be specified to prevent object deletion unless the current object version on the server matches the version indicated by the _etag value_.

for which a successful response is:

    204 No Content
    
An implementation SHOULD NOT allow deletion of objects with existing
object versions. It is RECOMMENDED that an implementation treat
deletion as a bulk request by the same client to delete all versions
of the object prior to deleting the object itself, however an
implementation MAY signal an error when object versions exist.  It is
further RECOMMENDED that such a deletion be processed atomically,
considering all client privileges in advance, but an implementation
MAY partially delete versions before failing due to an authorization
error on some subset of the versions.

Typical DELETE error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous deletion of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to delete the resource.
- **404 Not Found**: the name does not denote an existing resource.
- **409 Conflict**: the resource cannot be deleted at this time,
    i.e. because object versions still exist.
- **412 Precondition Failed**: the deletion was aborted because the current object version on the server does not match the version indicated by the `If-Match` request header.

## Object Version Resources

Any versioned object name in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _namespace path_ / _object name_ : _version id_
  
Where _version id_ is the service-issued identifier for a particular
version of the named object.

### Object Version Creation

See the previous section on Object Creation and Update. Object
versions are created by performing an update on the unversioned object
URL.

### Object Version Retrieval

A particular version of an object can be retrieved using the GET
operation whether or not it is the current version of the object:

    GET /namespace_path/object_name:version_id
    Host: authority_name
	If-None-Match: etag_value

for which the successful response is:

    200 OK
    Content-Type: content_type
    Content-MD5: hash_value
	Content-SHA256: hash_value
	Content-Disposition: filename*=UTF-8''filename
    Content-Length: N
	ETag: etag_value
    
    ...content...
    
with the same interpretation as documented for Object Retrieval above.

The `ETag` and `If-None-Match` headers allow client-side caching of object versions. Because a Hatrac object version is immutable, the _etag value_ for a given object version SHOULD NOT change over its lifetime.

### Object Version Metadata Retrieval

Metadata for a particular version of an object can be retrieved using
the HEAD operation whether or not it is the current version of the
object:

    HEAD /namespace_path/object_name:version_id
    Host: authority_name

for which the successful response is:

    200 OK
    Accept-Ranges: bytes
    Content-Type: content_type
    Content-MD5: hash_value
	Content-SHA256: hash_value
	Content-Disposition: filename*=UTF-8''filename
    Content-Length: N
    
with the same interpretation as documented for Object Metadata
Retrieval above.

### Object Version Deletion

The DELETE operation is used to delete an object version

    DELETE /namespace_path/object_name:version_id
    Host: authority_name

for which a successful response is:

    204 No Content

For completeness in the protocol, an `If-Match` header MAY be specified to control deletion of object versions, but it is redundant since object versions are immutable and their content cannot be in a different state than observed on a previous access.

Typical DELETE error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous deletion of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to delete the resource.
- **404 Not Found**: the name does not denote an existing resource.
- **412 Precondition Failed**: the `If-Match` request header was specified with an _etag value_ which does not match this object version.

Versions of objects can be deleted whether or not they are the current
version:

  - Deletion of any version MUST make that version unavailable.
  - Deletion of any version MAY permanently discard content. An
    implementation MAY retain deleted content to allow restoration
    procedures not documented here.
  - Deletion of the current version will cause the next most recent
    version of the object to become its new current version.
  - An object may be left empty, i.e. with no current version, if all
    versions have been deleted.  A subsequent update can reintroduce
    content for the object.

## Metadata Sub-Resources

The service also exposes sub-resources for metadata management on
existing object versions:

- https:// _authority_ / _resource name_ ;metadata
- https:// _authority_ / _resource name_ ;metadata/ _fieldname_

Where _resource name_ is currently restricted to object version names
as described above. The _fieldname_ is a lower-case string which
matches an HTTP request header suitable for describing content
metadata. The currently recognized _fieldnames_ include:

- `content-type`
- `content-disposition`
- `content-md5`
- `content-sha256`

### Lifecycle and Ownership

Metadata are sub-resources of the main resource identified in the
_resource name_ in the URL, and their lifetime is bounded by the
lifetime of that main resource.

1. Initial metadata MAY be specified during object creation and update.
2. Immutable checksums MAY be added on existing object versions.
3. Mutable metadata MAY be added, removed, or modified on existing object versions.

### Metadata Collection Retrieval

The GET operation is used to retrieve all metadata sub-resources en masse
as a document:

    GET /resource_name;metadata
	Host: authority_name
	Accept: application/json
	If-None-Match: etag_value
	
for which the successful response is:

    200 OK
	Content-Type: application/json
	Content-Length: N
	ETag: etag_value
	
	{"content-type": content_type, 
	 "content-md5": hash_value,
	 "content-sha256": hash_value,
	 "content-disposition": disposition}

The standard
[object version metadata retrieval](#object-version-metadata-retrieval),
operation uses the `HEAD` method on the main resource to retrieve this
same metadata as HTTP response headers.

### Metadata Value Retrieval

The GET operation is used to retrieve one metadata sub-resource as a
text value:

    GET /resource_name;metadata/fieldname
	Host: authority_name
	Accept: text/plain
	If-None-Match: etag_value
	
for which the successful response is:

    200 OK
	Content-Type: text/plain
	Content-Length: N
	ETag: etag_value
	
	value

The textual _value_ is identical to what would be present in the HTTP
response header value when retrieving the main resource content.

### Metadata Value Creation and Update

The PUT operation is used to create or update one metadata sub-resource as a
text value:

    PUT /resource_name;metadata/fieldname
	Host: authority_name
	Content-Type: text/plain
	If-Match: etag_value

	value

for which the successful response is:

    204 No Content

The textual _value_ is identical to what would be present in the HTTP
request header value when creating the main resource content.

### Metadata Value Deletion

The DELETE operation is used to create or update one metadata sub-resource as a
text value:

    DELETE /resource_name;metadata/fieldname
	Host: authority_name

for which the successful response is:

    204 No Content

## Access Control List Sub-Resources

An access control policy sub-resource has an HTTPS URL of the form:

- https:// _authority_ / _resource name_ ;acl
  - (for a list of all ACLs on the resource)
- https:// _authority_ / _resource name_ ;acl/ _access_
  - (for a specific ACL)
- https:// _authority_ / _resource name_ ;acl/ _access_ / _entry_
  - (for a specific ACL entry)

Where _resource name_ is a namespace, object, or object version name
as described above, _access_ is a specific access mode that is
applicable to that type of resource, and _entry_ is a specific _role_
name or the `*` wildcard.  The full set of access control lists for
each resource type is:
- Namespace
  - `owner`: lists roles considered to be owners of the namespace.
  - `create`: lists roles permitted to create new children in the namespace.
  - `subtree-owner`: lists roles considered to be owners of any namespace, object, or object version beneath the namespace.
  - `subtree-create`: lists roles permitted to create new children of the namespace or of any namespace beneath the namespace.
  - `subtree-update`: lists roles permitted to update data on any object beneath the namespace.
  - `subtree-read`: lists roles permitted to read any object version beneath the namespace.
- Object
  - `owner`: lists roles considered to be owners of the object.
  - `update`: lists roles permitted to update object with new versions.
  - `subtree-owner`: lists roles considered to be owners of any object version for the object.
  - `subtree-read`: lists roles permitted to read any object version for the object.
- Object Version
  - `owner`: lists roles considered to be owners of the object version.
  - `read`: lists roles permitted to read the object version.

### Lifecycle and Ownership

Access control lists are sub-resources of the main resource identified
by the _resource name_ in the URL, and they exist for the entire
lifetime of the main resource.

1. The root namespace is configured out of band with initial ACL
   content when a service is deployed.
1. When a client creates a nested namespace or a new object, the
   ownership of the new resource is set to the authenticated client by
   default, but the client may specified an alternative owner list as
   part of the creation request.  In the case of a new object, the
   initial object version gets the same ACL settings as the newly
   created object. **TODO:** define header to control initial ACLs
   during PUT.

### Access Control Retrieval

The GET operation is used to retrieve ACL settings en masse:

    GET /resource_name;acl
    Host: authority_name
    Accept: application/json
	If-None-Match: etag_value
    
for which the successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N
	ETag: etag_value
    
    {"access": ["role", ...], ...}
    
where response contains a JSON object with one field per _access_ mode
and an array of _role_ names and/or the `*` wildcard for each such access
list.

The HEAD operation can likewise retrieve en masse ACL metadata:

    HEAD /resource_name;acl
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N

### Access Control List Retrieval

The GET operation is also used to retrieve a specific ACL:

    GET /resource_name;acl/access
    Host: authority_name
    Accept: application/json
	If-None-Match: etag_value
    
for which the successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N
	ETag: etag_value
    
    ["role",...]

where the response contains just one array of _role_ names or the `*`
wildcard.

The HEAD operation can likewise retrieve individual ACL metadata:

    HEAD /resource_name;acl/access
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N

### Access Control List Entry Retrieval

The GET operation is also used to retrieve a specific ACL entry:

    GET /resource_name;acl/access/role
    Host: authority_name
    Accept: application/json
	If-None-Match: etag_value
    
for which the successful response is:

    200 OK
    Content-Type: text/plain
    Content-Length: N
	ETag: etag_value
    
    role

where the response contains just one _role_ name or `*` wildcard entry.

The HEAD operation is also used to retrieve metadata for a specific
ACL entry:

    HEAD /resource_name;acl/access/entry
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Content-Type: text/plain
    Content-Length: N

For all of the ACL sub-resource retrieval operations, an `If-None-Match` header MAY be specified with an _etag value_ to indicate that the client already possesses a copy of the sub-resource which was returned with an `ETag` header containing that same _etag value_. This is useful for cache control. The _etag value_, if returned by the server, MUST indicate a specific configuration of the ACL sub-resource such that proper caching and precondition-protected updates are possible using the related HTTP protocol features.

Typical GET error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous retrieval of such a policy is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to retrieve the policy.
  - **404 Not Found**: the namespace or object resource or ACL
      subresource is not found.
  - **304 Not Modified**: the current state of the ACL sub-resource matches the _etag value_ specified in the `If-None-Match` request header.

### Access Control List Update

The PUT operation is used to rewrite a specific ACL:

    PUT /resource_name;acl/access
    Host: authority_name
    Content-Type: application/json
    If-Match: etag_value
    
    ["role", ...]

The optional `If-Match` header MAY be specified with the _etag value_ corresponding to the last retrieved ACL sub-resource configuration, in order to prevent update in the case that another client has simultaneously updated the same ACL sub-resource while this request was being prepared and submitted.

The successful response is:

    204 No Content

where the input JSON array completely replaces the existing ACL.

It is RECOMMENDED that the implementation reject changes
which would strip too many permissions, e.g. leaving a resource with
no `owner`.

The PUT operation is also used to add one entry to a specific ACL:

    PUT /resource_name;acl/access/entry
    Host: authority_name

for which the successful response is:

    204 No Content

where the _entry_ role name or `*` wildcard is now present in the ACL.
   
Typical PUT error responses would be:
- **400 Bad Request**: the resource cannot be updates as requested,
    i.e. because insufficient permissions would remain.
- **401 Unauthorized**: the client is not authenticated and
  anonymous update of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to update the resource.
- **404 Not Found**: the name does not denote an existing resource.
- **412 Precondition Failed**: the update was aborted because the ACL sub-resource state on the server did not match the _etag value_ present in an `If-Match` request header.

### Access Control List Deletion

The DELETE operation is used to clear a specific ACL:

    DELETE /resource_name;acl/access
    Host: authority_name
    If-Match: etag_value

The optional `If-Match` header MAY be specified with the _etag value_ corresponding to the last retrieved ACL sub-resource configuration, in order to prevent deletion in the case that anohter client has simultaneously updated the same ACL sub-resource while this request was being prepared and submitted.
    
The successful response is:

    204 No Content

where the ACL is now empty.

It is RECOMMENDED that the implementation reject changes which would
strip too many permissions, e.g. leaving a resource with no `owner`.

The DELETE operation is also used to remove one entry from a specific ACL:

    GET /resource_name;acl/access/entry
    Host: authority_name

for which the successful response is:

    204 No Content

where the _entry_ role name or `*` wildcard is no longer present in
the ACL.
   
Typical DELETE error responses would be:
- **400 Bad Request**: the resource cannot be changed as requested,
    i.e. because insufficient permissions would remain.
- **401 Unauthorized**: the client is not authenticated and
  anonymous update of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to update the resource.
- **404 Not Found**: the name does not denote an existing resource.
- **412 Precondition Failed**: the deletion was aborted because the ACL sub-resource state on the server did not match the _etag value_ present in an `If-Match` request header.

## Chunked Upload Resources

To efficiently support restartable transfer for very large object
content, a stateful job management sub-resource exposes URLs for the
form:

- https:// _authority_ / _namespace path_ / _object name_ ;upload
  - (the set of upload jobs for a given object)
- https:// _authority_ / _namespace path_ / _object name_ ;upload / _job id_
  - (a single upload job)
- https:// _authority_ / _namespace path_ / _object name_ ;upload / _job id_ / _chunk number_
  - (a single chunk of data)

where _job id_ is a service-issued identifier for one transfer job,
and _chunk number_ is a zero-based ordinal for the chunk within the
series of chunks where chunk number _n_ starts at byte-offset _n_ \*
_K_ for a job using _K_ byte chunk size.

To allow different implementations, the upload job processes a set of
chunks of equal size determined at the time the job is
created. Arbitrary byte offsets are *not* allowed. The final chunk may
be less than the chunk size to account for arbitrary length jobs.

The three-phase chunked upload job has an effect equivalent to a
single PUT request on an object:

1. Create service-side transfer job state
1. Send set of idempotent chunks
1. Signal job completion

The benefit of this technique is that individual HTTP requests can be
kept to a reasonable size to prevent timeouts, and in the face of
transient communication failures the data transfer can easily resume
in the middle.  Only chunks that were partially in flight need to be
retransmitted.

This interface has been designed to accomodate two important
implementation strategies:
- The fixed chunk size and ordinal position can be used to compute a
  byte offset for direct assembly of data into sparse files in a
  filesystem.  The chunks are non-overlapping byte ranges at fixed
  offsets. Idempotent retransmission of chunks is permitted, but a
  client SHOULD NOT send different content for multiple requests using
  the same _chunk number_. An implementation MAY mix content of multiple
  transmissions for the same _chunk number_.  An implementation MAY accept
  completion of an upload job that has missing chunks.
- The individual requests easily map to similar chunked upload
  interfaced in object systems such as Amazon S3, allowing a thin
  proxy to implement Hatrac on top of such services. Retransmission or
  out-of-order transmission of chunks is permitted, but a client
  SHOULD NOT skip any chunks. An implementation MAY reject
  completion of an upload job that has missing chunks.

Hence, it is the client's responsibility to track acknowledged of
individual chunk transfers and defer completion of an upload job until
all chunks have been successfully transmitted.

### Chunked Upload Job Creation

The POST operation is used to create a new upload job:

    POST /namespace_path/object_name;upload
    Host: authority_name
    Content-Type: application/json
    
    {"chunk-length": K, 
     "content-length": N,
     "content-type": "content_type",
     "content-md5": "hash_value",
	 "content-sha256": "hash_value",
	 "content-disposition": "disposition"}

where the JSON attributes `chunk-length` and `content-length` are
mandatory to describe the shape of the data upload, while
`content-type`, `content-disposition`, `content-md5`, and
`content-sha256` are optional and provide additional metadata for the
completed object, with the same semantics as if the object had been
created as a simple object (without the upload job API) and those same
fields had been provided as HTTP PUT request headers. For backwards
compatibility, these JSON attribute names are also supported as
aliases:

- `chunk_bytes`: deprecated alias for `chunk-length`
- `total_bytes`: deprecated alias for `content-length`
- `content_md5`: deprecated alias for `content-md5`

As with object and namespace creation, an optional query parameter may
be supplied to request automatic creation of ancestor namespaces:

    POST /namespace_path/object_name;upload?parents=true
	...

In either case, the successful response is:

    201 Created
    Location /namespace_path/object_name;upload/job_id
    Content-Type: text/uri-list
    Content-Length: N
    
    /namespace_path/object_name;upload/job_id

where the new job is ready to receive data chunks.

Typical PUT error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous creation of a job is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to create the job.
  - **409 Conflict**: the object name is unavailable for such use.

### Chunked Upload Job Listing Retrieval

The GET operation is used to list pending upload jobs on an object:

    GET /namespace_path/object_name;upload
    Host: authority_name

where the successful response is a JSON array of job URLs:

    200 OK
    Content-Type: application/json
    Content-Length: N
    
    ["/hatrac/namespace_path/object_name;upload/job_id", ...]

If the `text/uri-list` content-type is negotiated, the response will
be a whitespace separated list of job URLs:

    200 OK
	Content-Type: text/uri-list
	Content-Length: N
	
	/hatrac/namespace_path/object_name;upload/job_id1
	/hatrac/namespace_path/object_name;upload/job_id2
    ...

### Chunk Upload

The PUT operation is used to send data chunks for an existing job:

    PUT /namespace_path/object_name;upload/job_id/chunknumber
    Host: authority_name
    Content-Type: application/octet-stream
    Content-Length: K
    
    ...data...
    
for which the successful response is:

    204 No Content
    
where the data was received and stored.

Typical PUT error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous upload of the chunk is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to upload the chunk.
  - **400 Bad Request**: the chunk number is not a non-negative integer.
  - **409 Conflict**: the chunk number is too large for the defined job.

### Chunked Upload Job Finalization

The POST operation is used to signal completion of an upload job:

    POST /namespace_path/object_name;upload/job_id
    Host: authority_name
    
for which the successful response is:

    201 Created
    Location: /namespace_path/object_name:version_id
    Content-Type: text/uri-list
    Content-Length: N
    
    /namespace_path/object_name:version_id

where `Location` includes the URL of the newly created object version
that is comprised of all the uploaded data chunks as if it had been
created by a corresponding PUT request:

    PUT /namespace_path/object_name
    Host: authority_name
    Content-Type: content_type
    Content-MD5: hash_value
    Content-Length: N
    
    ...content...

Typical POST error responses would be:
  - **401 Unauthorized** the client is not authenticated
  - **403 Forbidden** the client is authenticated but does not have
    sufficient privilege to finalize the upload.
  - **409 Conflict** the currently uploaded content does not match the
    `Content-MD5` header of the original upload job. An implementation
    MAY skip this validation but it is RECOMMENDED to perform this
    validation rather than create broken objects.

### Chunked Upload Job Status Retrieval

The GET operation is used to view the status of a pending upload:

    GET /namespace_path/object_name;upload/job_id
    Host: authority_name
    
for which the successful response is:

    200 OK
    Content-Type: application/json
    Content-Length: N
    
    {"url": "/namespace_path/object_name;upload/job_id",
     "owner": ["role"...],
     "target": "/namespace_path/object_name"
     "chunk-length": K,
	 "content-length": N,
	 ...
	}
     
summarizing the parameters set when the job was created including
optional object metadata such as `content-type`. Note, there is no
support for determining which chunks have or have not been uploaded as
such tracking is not a requirement placed on Hatrac implementations.

### Chunked Upload Job Cancellation

The DELETE method can be used to cancel an upload job that has not yet
been finalized:

    DELETE /namespace_path/object_name;upload/job_id
    Host: authority_name
    
for which the successful response is:

    204 No Content

Once canceled, the job resource no longer exists and associated
storage SHOULD be reclaimed.

