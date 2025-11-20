
This document summarizes some internal database state for developers or
expert service administrators.

## Version Aux Column

The `aux` column of the `hatrac.version` table stores a JSON formatted
value that can override some service behaviors. It is typically empty
(`null`) in a basic deployment scenario.

If populated, it should be a JSON object with a sparse set of
key-value pairs. When present, these keys introduce special
behavior. They are detected and handled in the following priority
order, so the first detected field may change behavior before other
fields can be processed:

1. `rename_to`: preferred name and version key to service content.
2. `url`: a URL to the version content at a remote hatrac service.
3. `hname` and `hversion`: name and version to override URL parsed values.
4. `version`: version to override backend storage version keying.

The `rename_to` field stores a pair `[` _hname_ `,` _hversion_ `]` which
is used to lookup a preferred object version that obsoletes the
annotated object version. The service resolves this reference (similar
to a symbolic link in a filesystem) and performs the actual content
retrieval via the record found with that _hname_ and _hversion_. Access
control is processed using the preferred version and the HTTP
`Location` response header is also set to identify the preferred name.

The `url` field triggers an HTTP redirect to a remote Hatrac object
version that should have the same content. This is primarily used
during an online migration from an old to new server with the
`hatrac-migrate` utility script.

The `hname` and `hversion` fields override the default behavior when
retrieving content from the storage backend. The default behavior is
to use the actual `name` and `version` columns of the respective
Hatrac database records as input to the addressing function of the
storage backend. The `h` prefix means the "Hatrac" value as parsed
from URLs.

The `version` field overrides the backend storage version ID,
currently only meaningful in the S3 backend. This is relevant when
accessing a versioned bucket, where the addressing function maps the
Hatrac name and version values (e.g. from the URL) to an object key
but there might be a different version ID to access the correct
version of the backend object.


## Object Renaming

The object renaming feature (achieved with POST requests passing the
`{"command": "rename_from", ...}` batch command description) are
implemented by making coordinated changes to the `aux` column fields
described above:

1. A new version record is created under the new/preferred name with
its `hname`, `hversion`, and `version` aux fields set to refer to the
existing backend storage content addressed by the old/legacy name in
use when it was actually stored.

2. The old version record has its `rename_to` aux field set to point
to the new/preferred version record.

During migration, existing object renaming is slightly normalized:

1. The content is transferred and stored under the new/preferred name,
rather than recreating the content under the old/legacy storage address.

2. The old/legacy records are kept with `rename_to` so that they
continue to allow HTTP access via legacy URLs.


### Deletion with Renaming

All deletion permutations are allowed with different results.

1. A rename_from source can be deleted and it only deletes the DB
entry while not touching the backing storage which is owned by the new
rename_to target. The target can still be accessed.

2. A rename_to target can be deleted and it deletes the DB entry and
the original backing storage which it owns. The rename_from source DB
entry exists and can itself be managed or deleted, but will raise 409
errors on attempts to GET the content.
