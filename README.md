# Hatrac

[Hatrac](http://github.com/informatics-isi-edu/hatrac) (pronounced
"hat rack") is a simple object storage service for web-based,
data-oriented collaboration. It presents a simple HTTP RESTful service
model with:
- **H**ierarchical data naming
- **A**ccess control suitable for collaboration
- **T**rivial support for browser-based applications
- **R**eferential stability for immutable data
- **A**tomic binding of names to data
- **C**onsistent use of distributed data

## Status

[![Build Status](https://github.com/informatics-isi-edu/hatrac/actions/workflows/main.yml/badge.svg)](https://github.com/informatics-isi-edu/hatrac/actions/workflows/main.yml)

Hatrac is research software but runs in production for several
informatics projects using the filesystem storage backend. It is
developed in a continuous-integration style with an automated
regression test suite covering all core API features.

## Using Hatrac

As a protocol, the [Hatrac REST API](docs/REST-API.md) can be easily
accessed by browser-based applications or any basic HTTP client
library. The API can also be easily re-implemented by other services
if interoperability is desired. As a piece of software and reference
implementation it targets two deployment scenarios:

1. A standalone Linux Apache HTTPD server with local filesystem
   storage of data objects and PostgreSQL storage of namespace and
   policy metadata.
1. An Amazon AWS scalable service with S3 storage of data objects and
   RDS PostgreSQL storage of namespace and policy metadata.

Both scenarios share much of the same basic software stack, though
additional administrative effort is required to assemble a scalable
deployment.

### Prerequisites

Hatrac is developed and tested primarily on an enterprise Linux
distribution with Python 2.7. It has a conventional web service stack:
- Apache HTTPD
- mod_wsgi 
- web.py lightweight web framework
- psycopg2 database driver
- PostgreSQL 9.5 or newer
- webauthn2 security adaptation layer (another product of our group)

### Installation

There is not much installation automation yet. Please see our [detailed installation instructions](docs/INSTALL.md)

### Operational Model

1. The HTTP(S) connection is terminated by Apache HTTPD.
1. The Hatrac service code executes as the `hatrac` daemon user.
1. The service configuration is loaded from `~hatrac/hatrac_config.json`:
  - Security configuration
  - Storage back-end.
1. All object naming and detailed policy metadata is stored in the RDBMS.
1. All bulk object data is stored in the configured storage backend.
1. Client authentication context is determined by callouts to the webauthn2 module:
  - Client identity
  - Client roles/group membership.
1. Authorization of service requests is determined by the service code:
  - ACLs retrieved from RDBMS
  - ACLs are intersected with authenticated client context.
1. The RDBMS and backend storage are accessed using deployed service
   credentials which have no necessary relation to client security
   model.

## Why Another Storage Service?

The purpose of Hatrac is to facilitate data-oriented collaborations.
To understand our goals requires an understanding of what we mean by
_data-oriented architecture_ and what we mean by _data-oriented
collaboration_.  With that understanding, we can then consider
specific examples of object storage systems and their suitability to
purpose.

### Data-Oriented Architecture

Data orientation, like service orientation, is a philosophy for
decomposing a complex system into modular pieces.  These pieces, in
turn, are meant to be put back together in novel
combinations. However, the nature of the pieces and the means of
recombination are different:

1. **Service orientation**: a _service_ encapsulates a (possibly
   hidden) _state model_ and a set of _computational behaviors_ behind
   a _message-passing_ interface which can trigger computation and
   state mutation.  Over time, new services may be developed to
   synthesize a behavior on top of existing services, and compatible
   services may be developed to support the same message protocol
   while having differences in their internal computation or state.
1. **Data orientation**: a universe of _actors_ take on roles of
   _producing_, _referencing_, and/or _consuming_ data
   _objects_. There is a basic assumption that actors will somehow
   discover and consume data objects, leading them to synthesize new
   data products.  Over time, the set of available data evolves as a
   result of the combined activity of the community of
   actors.
   
Crucially, data itself is the main shared resource and point of
integration in a data-oriented architecture. This may include digests,
indices, and other derived data products as well as the synthesis and
discovery of new products.  Services with domain-specific message
interfaces and "business logic" are considered to be transient just
like any other actor; they cannot be relied upon to mediate access to
data over the long term. Rather, community activity causes the data to
evolve over time while individual services, applications, and actors
come and go.  The data artifacts are passed down through time, much
like the body of literature and knowledge passed through human
civilization.  This requires different mechanisms for the collection
and dissemination of data among a community while remaining agnostic
to the methods and motivations of the actors.

### Data-Oriented Collaboration Semantics for Object Storage

To simplify the sharing of data resources while remaining agnostic to
the methods and motivations of actors, we choose a simple
collaborative object sharing semantics:

1. **Generic** bulk data: objects have a generic byte-sequence
   representation that can be further interpreted by actors aware of
   the encoding or data model used to produce the object.
1. **Atomic** object creation and naming: an object becomes visible
   all at once or not at all.
1. **Immutable** objects and object references: any reference to or
   attempt to retrieve an object by name always denotes the same
   object content, once it is defined.
1. **Accountable** creation and access: policies can be enforced to
   restrict object creation or retrieval within a community of
   mutually trusting actors.
1. **Delegated** trust: the coordination of naming and policy
   management can be delegated among community members to allow for
   self-help and a lower barrier to entry.
1. **Hierarchical** naming: perhaps less important than the preceding
   requirements, we find that many users are more comfortable with a
   hierarchical namespace which can encode some normalized
   semantic information about a set of objects, provide a focal point
   for collaboration tasks and conventions, and scope policy that
   relates to a smaller sub-community.

It is important to note that we restrict Hatrac to bulk data where a
generic byte-sequence representation makes sense. We realize that
effective collaboration may benefit from structured metadata and
search facilities, but these are considered out of scope for
Hatrac. We believe that these object storage semantics are _necessary_
but _not sufficient_ for data-oriented collaboration, and we are
simultaneously exploring related concepts for data-oriented search and
discovery.

### Pragmatic Data-Oriented Implementation Requirements

To support a range of collaboration scenarios we have observed with
our scientific peers, we adopt several additional implementation
requirements:

1. Integrate into conventional web architectures
   - Use URL structure for naming data objects in a federated universe
   - Use HTTP protocol for accessing and managing data objects
2. Flexible deployment scenarios
   - Run in a traditional server or workstation for small groups with local resources
   - Run in a conventional hosted/colocated server
   - Run in a cloud/scale-out environment
3. Configurable client identity and role providers
   - Standalone database
   - Enterprise directory
   - Cloud-hosted identity and group providers
4. Configurable storage and "bring your own disks" scenarios
   - Store objects as files in a regular filesystem
   - Store objects in an existing object system (such as Amazon S3)
   - Allow communities to mix and match these options

### Why not Amazon S3?

Amazon's Simple Storage Service (S3) can provide the sharing semantics
we seek when object versioning is enabled on a bucket. However, it has
drawbacks as far as deployment options, security integration, and ease
of use:

- There is no simple standalone server option for groups who wish to
  locally host their data and "bring their own disks".
- The access control model in S3 is with respect to Amazon AWS
  accounts and does not allow simple integration with other user
  account systems.
- Awkward support for hierarchical naming, and policy.

Of course, we support S3 as a storage target. The purpose of Hatrac is
to enable the collaboration environment on top of conventional storage
options like S3 which are more focused on providing infrastructure
than coordinating end-user collaboration.

### Why not file-sharing services like Dropbox?

File-sharing services such as Dropbox make it trivial to share data
files between users but provide several challenges when looking at
larger collaboration:

- Lack of immutability guarantees to provide stable references to
  data.
- All or nothing trust model means a user invited into the shared
  folder has full privilege to add, delete, or modify content.
- Shared/replication model means that every user potentially downloads
  the entire collection which may be inconvenient or impractical for
  large, data-intensive collaborations.

### Why not a basic Linux Apache server?

A web server such as Apache HTTPD provides many convenient options for
security integration and download of objects. However, there is a
large gap in supporting upload or submission of new objects and
management of object policy by community members. Historically, this
has been handled in an ad hoc fashion by each web service implemented
on top of Apache.  As such an add-on service, Hatrac provides the
minimal data-oriented service interface we seek on top of Apache
HTTPD, so we can build different services and applications that share
data resources without adding a private, back-end data store to each
one.

## Help and Contact

Please direct questions and comments to the [project issue
tracker](https://github.com/informatics-isi-edu/hatrac/issues) at
GitHub.

## License

Hatrac is made available as open source under the Apache License,
Version 2.0. Please see the [LICENSE file](LICENSE) for more
information.

## About Us

Hatrac is developed in the
[Informatics group](http://www.isi.edu/research_groups/informatics/home)
at the [USC Information Sciences Institute](http://www.isi.edu).
