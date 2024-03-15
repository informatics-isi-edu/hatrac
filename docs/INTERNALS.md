
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
3. `name` and `version`: name and version key to use with the storage backend.

The `rename_to` field stores a pair `[` _name_ `,` _version_ `]` which
is used to lookup a preferred object version that obsoletes the
annotated object version. The service follows this reference (similar
to a symbolic link in a filesystem) and performs the actual content
retrieval via the version found with that _name_ and _version_. Access
control is processed using the preferred version and the HTTP
`Location` response header is also set to identify the preferred name.

The `url` field triggers an HTTP redirect to a remote Hatrac object
version that should have the same content. This is primarily used
during an online migration from an old to new server with the
`hatrac-migrate` utility script.

The `name` and `version` fields override the default behavior when
retrieving content from the storage backend. The default behavior is
to use the actual `name` and `version` columns of the respective
Hatrac database records when addressing the storage backend.

## Object Renaming

The object renaming feature (achieved with POST requests passing the
`{"command": "rename_from", ...}` batch command description) are
implemented by making coordinated changes to the `aux` column fields
described above:

1. A new version record is created under the new/preferred name with
its `name` and `version` aux fields set to refer to the existing
backend storage content addressed by the old/legacy name in use when
it was actually stored.

2. The old version record has its `rename_to` aux field set to point
to the new/preferred version record.

During migration, existing object renaming is slightly normalized:

1. The content is transferred and stored under the new/preferred name,
rather than recreating the content under the old/legacy storage address.

2. The old/legacy records are kept with `rename_to` so that they
continue to allow HTTP access via the same logic as the pre-migration
system.
