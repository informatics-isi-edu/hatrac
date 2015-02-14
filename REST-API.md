# Hatrac REST API

[Hatrac](http://github.com/informatics-isi-edu/hatrac) (pronounced
"hat rack") is a simple object storage service for web-based,
data-oriented collaboration. It presents a simple HTTP REST API with
the following characteristics:

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
hierarchical name has a simple three-phase lifecycle:
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

### TBD: Listing of Namespace Contents?

It is undecided whether there is a need to list children of the root
namespace.

## Nested Namespace Resources

Any hierarchical namespace in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _parent path_ / _namespace id_

Where _parent path_ is the name of the enclosing namespace and
_namespace id_ is the relative name of the nested namespace. Of
course, the enclosing namespace may be the root namespace of the
deployment or another nested namespace.

### Nested Namespace Creation

The PUT operation is used to create a new nested namespace.  A simple
JSON representation of the namespace configuration is provided as input:

    PUT /parent_path/namespace_id
    Host: authority_name
    Content-Type: application/json
    
    {"hatrac-namespace": true}

for which a successful response is:

    201 Created
    Location: /parent_path/namespace_id
    
**Note**: see related object resource interface for pragmatic discussion
of the use of Content-Type and content to disambiguate namespace and
object creation requests.

Typical PUT error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous creation of such a namespace is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to create the namespace.
- **409 Conflict**: the namespace cannot be created due to a
  conflict with existing state of the service:
  - The _parent path_ does not denote a namespace
  - The namespace already exists

### Nested Namespace Deletion

The DELETE operation is used to delete a nested namespace

    DELETE /parent_path/namespace_id
    Host: authority_name

for which a successful response is:

    200 OK
    
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

### Listing of Namespace Contents?

See same topic under Root Namespace Resource sub-section.

## Object Resources

Any unversioned object name in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _namespace path_ / _object name_
  
Where _namespace path_ is the name of the enclosing namespace and
_object name_ is the relative name of the object.

### Object Creation and Update

The PUT operation is used to create a new object or a new version of
an existing object.  A simple JSON representation of the namespace
configuration is provided as input:

    PUT /namespace_path/object_name
    Host: authority_name
    Content-Type: content_type
    Content-Length: N
    
    ...content...

for which a successful response is:

    201 Created
    Location: /namespace_path/object_name:version_id
    
Typical PUT error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous creation of such an object is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to create the object.
  - **409 Conflict**: the object cannot be created due to a
      conflict with existing state of the service:
    - The _namespace path_ may not denote a namespace
    - The _object name_ may already be in use as a namespace,
      therefore preventing its use as an object.

**Note**: There is ambiguity in the meaning of a URL when creating a
new object or nested namespace because they have the same syntactic
structure.  Hatrac disambiguates such requests in a decision process:
1. If the full path denotes an existing object, the PUT request MUST
   denote a request to update the content of the existing object,
   regardless of what `Content-Type` and content is present.
1. If `Content-Type` is any value other than `application/json`, the
   PUT request MUST denote a request to create a new object with the
   specified content.
1. If `Content-Type` is `application/json` **and** the input JSON
   content contains the field `hatrac-namspace` with value `true`, the
   PUT request MUST denote a request to create a new nested namespace.
1. Any other PUT request not matching the above is considered an
   object creation request.

This set of rules makes it simple to create any common object or
namespace. In the degenerate case where one wishes to create an object
with a JSON content that looks exactly like a namespace request input,
the solution is to first create an empty object and then immediately
update its content with the desired JSON content.  We feel that this is
a reasonable trade-off for 

### Object Retrieval

The GET operation is used to retrieve the current version of an object

    GET /namespace_path/object_name
    Host: authority_name
    Accept: *
    
for which a successful response is:

    200 OK
    Location: https://authority_name/namespace_path/object_name:version_id
    Content-Type: content_type
    Content-Length: N
    
    ...content...
    
Typical GET error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous retrieval of such an object is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to retrieve the object.
  - **404 Not Found**: the name does not denote a defined object.
  - **409 Conflict**: the object cannot be retrieved at this time,
      e.g. there are currently no object versions defined.

### Object Deletion

The DELETE operation is used to delete an object

    DELETE /namespace_path/object_name
    Host: authority_name

for which a successful response is:

    200 OK
    
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

## Object Version Resources

Any versioned object name in Hatrac has an HTTPS URL of the form:

- https:// _authority_ / _namespace path_ / _object name_ : _version id_
  
Where _version id_ is the service-issued identifier for a particular
version of the named object.

### Object Version Creation

See the previous section on Object Creation and Update. Object
versions are created by performing an update on the unversioned object
URL.

### Object Version Deletion

The DELETE operation is used to delete an object version

    DELETE /namespace_path/object_name:version_id
    Host: authority_name

for which a successful response is:

    200 OK
    
Typical DELETE error responses would be:
- **401 Unauthorized**: the client is not authenticated and
  anonymous deletion of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to delete the resource.
- **404 Not Found**: the name does not denote an existing resource.

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
  - `create`: lists roles permitted to create new children in the
    namespace.
- Object
  - `owner`: lists roles considered to be owners of the object.
  - `update`: lists roles permitted to update the object with a new
    object version.
  - `read`: lists roles permitted to read new versions of the object
    by default.
- Object Version
  - `owner`: lists roles considered to be owners of the object
    version.
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
   created object.
1. When a client updates an existing object with a new object version,
   the new version gets ACL settings depending on the role of the
   authenticated client:
   - By default, the new object version has the same ownership and
     read permissions as the object being updated.
   - If the authenticated client holds a role listed as an owner of
     the object being updated, the client MAY override the ownership
     or read permissions of the new version as part of the request.

TBD: any forced ACL content inherited from parent namespace?

### Access Control Retrieval

The GET operation is used to retrieve ACL settings en masse:

    GET /resource_name;acl
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Location: https://authority_name/resource_name;acl
    Content-Type: application/json
    Content-Length: N
    
    {"access": ["role", ...], ...}
    
where response contains a JSON object with one field per _access_ mode
and an array of _role_ names and/or the `*` wildcard for each such access
list.

The GET operation is also used to retrieve a specific ACL:

    GET /resource_name;acl/access
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Location: https://authority_name/resource_name;acl/access
    Content-Type: application/json
    Content-Length: N
    
    ["role",...]

where the response contains just one array of _role_ names or the `*`
wildcard.

The GET operation is also used to retrieve a specific ACL entry:

    GET /resource_name;acl/access/entry
    Host: authority_name
    Accept: application/json
    
for which the successful response is:

    200 OK
    Location: https://authority_name/resource_name;acl/access/entry
    Content-Type: application/json
    Content-Length: N
    
    "role"

where the response contains just one _role_ names `*` wildcard entry.

Typical GET error responses would be:
  - **401 Unauthorized**: the client is not authenticated and
      anonymous retrieval of such a policy is not supported.
  - **403 Forbidden**: the client is authenticated but does not have
      sufficient privilege to retrieve the policy.
  - **404 Not Found**: the name does not denote a defined policy.

### Access Control Update

The PUT operation is used to rewrite ACL settings en masse:

    PUT /resource_name;acl
    Host: authority_name
    Content-Type: application/json
    
    {"access": ["role", ...], ...}
    
for which the successful response is:

    200 OK

where the input JSON completely replaces the existing policy, and any
missing _access_ field is treated as equivalent to the field being
present with an empty value `[]`, i.e. no authorized roles for that
access mode.

The PUT operation is also used to rewrite a specific ACL:

    PUT /resource_name;acl/access
    Host: authority_name
    Content-Type: application/json
    
    ["role", ...]
    
for which the successful response is:

    200 OK

where the input JSON array completely replaces the existing ACL.

It is RECOMMENDED that the implementation reject changes
which would strip too many permissions, e.g. leaving a resource with
no `owner`.

The PUT operation is also used to add one entry to a specific ACL:

    PUT /resource_name;acl/access/entry
    Host: authority_name

for which the successful response is:

    200 OK

where the _entry_ role name or `*` wildcard is now present in the ACL.
   
Typical PUT error responses would be:
- **400 Bad Request**: the resource cannot be updates as requested,
    i.e. because insufficient permissions would remain.
- **401 Unauthorized**: the client is not authenticated and
  anonymous update of such a resource is not supported.
- **403 Forbidden**: the client is authenticated but does not have
  sufficient privilege to update the resource.
- **404 Not Found**: the name does not denote an existing resource.


### Access Control Deletion

The DELETE operation is used to clear a specific ACL:

    DELETE /resource_name;acl/access
    Host: authority_name
    
for which the successful response is:

    200 OK

where the ACL is now empty.

It is RECOMMENDED that the implementation reject changes which would
strip too many permissions, e.g. leaving a resource with no `owner`.

The DELETE operation is also used to remove one entry from a specific ACL:

    GET /resource_name;acl/access/entry
    Host: authority_name

for which the successful response is:

    200 OK

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

## Chunked Upload Resources

To efficiently support restartable transfer for very large object
content, a stateful job management sub-resource exposes URLs for the
form:

- https:// _authority_ / _namespace path_ / _object name_ ;upload
  - (the set of upload jobs for a given object)
- https:// _authority_ / _namespace path_ / _object name_ ;upload / _job id_
  - (a single upload job)
- https:// _authority_ / _namespace path_ / _object name_ ;upload / _job id_ / _position_
  - (a single chunk of data)

where _job id_ is a service-issued identifier for one transfer job,
and _position_ is a zero-based ordinal for the chunk within the
overall transfer.

To allow different implementations, the upload job processes a set of
chunks of equal size determined at the time the job is created. The
final chunk may be less than the chunk size to account for arbitrary
length jobs.

The three-phase chunked upload job has an effect equivalent to a
single PUT request on an object:

1. Create service-side transfer job state
1. Send set of chunks
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
  filesystem.
- The individual requests easily map to similar chunked upload
  interfaced in object systems such as Amazon S3, allowing a thin
  proxy to implement Hatrac on top of such services.

### Chunked Upload Job Creation

The POST operation is used to create a new upload job:

    POST /namespace_path/object_name;upload
    Host: authority_name
    Content-Type: application/json
    
    {"hatrac-upload": true, "chunk-bytes": K, "total-bytes": N, "content-type": "content_type"}
    
for which the successful response is:

    201 Created
    Location /namespace_path/object_name;upload/job_id

where the new job is ready to receive data chunks.

### Chunk Upload

The PUT operation is used to send data chunks for an existing job:

    PUT /namespace_path/object_name;upload/job_id/position
    Host: authority_name
    Content-Type: application/octet-stream
    Content-Length: K
    
    ...data...
    
for which the successful response is:

    200 OK
    
where the data was received and stored.

### Chunked Upload Job Finalization

The PUT operation is used to signal completion of an upload job:

    PUT /namespace_path/object_name;upload/job_id
    Host: authority_name
    Content-Type: application/json
    
    {"hatrac-upload": true, "complete": true}
    
for which the successful response is:

    201 Created
    Location: /namespace_path/object_name:version_id

where `Location` includes the URL of the newly created object version
that is comprised of all the uploaded data chunks.
    
### Chunked Upload Job Cancellation

    PUT /namespace_path/object_name;upload/job_id
    Host: authority_name
    Content-Type: application/json
    
    {"hatrac-upload": true, "complete": true}
    
for which the successful response is:

    200 OK


