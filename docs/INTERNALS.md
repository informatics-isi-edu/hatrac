
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

## Incremental Polling Observer Pattern

For operations tooling, we would like to construct _polling
observers_: agents which can follow along with Hatrac DB state changes
and perform automated maintenance tasks. For example, a full listing
of object versions could be compared against an ERMrest catalog to
identify dangling references (to dead objects) or orphaned objects
(with no catalog reference). Or, coordinated access control policies
could be orchestrated between the two systems, with an obsever to
react to newly added objects, to detect drift in these policies, or to
proactively "heal" the policy.

A naive polling observer would periodically query Hatrac to enumerate
all namespaces, objects, and object versions. Using the core object
storage APIs, this would be quite inefficient and produce a large
volume of log traffic too. There are two extensions to Hatrac which
would help optimize this:

1. New bulk listing APIs to allow efficient, enumeration of Hatrac
`name` and `version` table content.

2. New temporal properties to allow ordering by change, so the bulk
APIs support paginated, incremental observation of state changes.

Together, these should allow a polling observer to get a bulk listing
of all Hatrac content, build a private replica, and fall into a
pattern of incrementally retrieving new or revised rows to update its
understanding of Hatrac content.

When the bulk API exposes tombstone records (where `is_deleted` is
true), the observer can detect deletion incrementally too.

NOTE: this approach retains one caveat of naive polling. It is not
log-based replication. An asynchronous, polling observer is not
guaranteed to see every mutation in a rapid burst. Some changes may be
ephemeral, overwritten before the observer can query each state. But,
we can guarantee that the observer will eventually see the quiescent
state. This is sufficient to support cleanup and drift detection, but
not a substitute for having appropriate access control, logging, and
audit capability.

## Temporal Properties

Temporal properties will be added to internal `name` and `version`
tables:
e
- `created_at`: The timestamptz when the row was created
- `modified_at`: The timestamptz when the row was last modified

Because of the immutable semantics of hatrac object-versions, we only
care to track changes to these database records, not the byte stream
(file) content in the backing object storage.

These two are analogous to the RCT/RMT columns in ERMrest. They
will reflect the human-readable, wall-clock time when namespace/object
or object-version records are created or updated, using the `now()`
timestamp available in SQL triggers.

When an existing hatrac DB is upgraded (via `hatrac-deploy`) to add
these columns, existing rows will be initialized as if they were all
created during the upgrade transaction.

### Sequencing Hazard Mitigation

PostgreSQL may add timestamps to a table out of sequence. The
timestamp is a moment when the transaction starts rather than when it
finishes. A slower transaction may commit its (older) timestamp after
other transactions have already committed their (newer) values. An
unlucky client could observe this asynchrony. Hence, the values
themselves are insufficient to support incremental monitoring for
change based on a "last seen" temporal cursor.

Thankfully, PostgreSQL exposes a useful snapshot concept, describing
active (non-retired) transactions. We can test whether the snapshot
`xmin` is less than the snapshot `xmax`, indicating there are
non-retired tansactions in progress. Conversely, when these values are
equal, the database is in a quiescent state and there is no hazard in
the observable chronology.

Because Hatrac is normally expected to quiesce in between mutation
requests, we can simply abort unlucky polling observer requests with a
`503 Service Unavailable` status code when a hazard is detected. If
the observer responds with backoff and retry, they should eventually
query at a quiescent moment in the future.

### Asynchronous and Continuous Replication

There is no transaction control to give the client a completely atomic
snapshot of remote Hatrac state. Instead, the client must retrieve
content from two different tabular sources and cope with transient
referential integrity errors during the retrieval process:

1. Loop over `version` table records until an empty page result.

2. Loop over `name` table records until an empty page result.

Once both streams have been exhausted in this order, the observer
holds a set of records they could merge into their own replica of
these Hatrac tables.

### Transient Inconsistencies in Replica Stream

While retrieving version records, there will be unresolvable
references to the name table, which hasn't been fetched yet. This is
the foreign key `version.nameid` which references `name.id`.

While retrieving name records, there may be unresolvable references
between name records. This is the foreign key `name.pid` which
references `name.id`. Once all name records have been fetched, the
observer is guaranteed to have observed all ancestor namespaces which
are references by other names they have observed.

Both of these referential integrity challenges are ephemeral, while
fetching records in order of last mutation, rather than insertion
order. But, this temporal order is necessary to allow mutation order
to be used as an ongoing stream position.

Each time the polling observer polls, they should repeat the same
process to fetch revisions to the version table, followed by fetching
revisions to the name table.

### Record Stream Pagination

We feed the following paramters for streaming of each table:

- `limit`: the number of rows to include in a page
- `last_modified_at`: the `modified_at` of the prior row
- `last_id`: the `id` of the prior row

The prior row material defines the stream position last seen by the
observer, whether it is the most recent change or just the most recent
that would fit in the page size.

To boostrap a new observer, use special position:

- `last_modified_at`: `-infinity`
- `last_id`: `0`

These are guaranteed to be earlier in the sort order than any record
content, so the first page will get the very earliest content.

The following SQL query templates represent the paginated streams with
these parameters:

    SELECT *
    FROM hatrac.name
    WHERE modified_at >= {{last_modified_at}}
      AND (modified_at > {{last_modified_at}} OR id > {{last_id}})
    ORDER BY modified_at, id
    LIMIT {{page_size}}

    SELECT *
    FROM hatrac.version
    WHERE modified_at >= {{last_modified_at}}
      AND (modified_at > {{last_modified_at}} OR id > {{last_id}})
    ORDER BY id
    LIMIT {{page_size}}

For robustness, the client SHOULD persistently store the `id` and
`modified_at` from the last row of the last page retrieved from each
table. To recover this state from replica tables held on the client
side, find the row with the stream position, e.g.

    SELECT modified_at, id
    FROM name_replica
    ORDER BY modified_at DESC, id DESC
    LIMIT 1;

    SELECT modified_at, id
    FROM version_replica
    ORDER BY modified_at DESC, id DESC
    LIMIT 1;
