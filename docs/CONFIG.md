
# Hatrac Configuration Manual

The service is configured with a `hatrac_config.json` file in the daemon account home directory, e.g. `/home/hatrac/hatrac_config.json` on typical deployments.

Some basic examples are included in the [Hatrac Installation document](INSTALL.md). This document is meant as more complete specification of configurable parameters.

## Core Service Config

The top-level JSON document has options which affect the service as a
whole:

```
{
  "service_prefix": <URL path prefix string>,
  "database_dsn": <database connection DSN string>,
  "allowed_url_char_class": <regular expression string>,
  "max_request_payload_size": <integer (default 134217728)>,
  "firewall_acls": { <aclname>: <acl>, ... },
  "read_only": <boolean>,
  "storage_backend": <backend name string>,
  "error_templates": { <error response template map...> },
  ...
}
```

### `service_prefix`

The service's URL path prefix, i.e. `"/hatrac"`. By default it is auto-configured from the WSGI environment.

This is a *developer* option which should not be used in practice.

### `database_dsn`

The connection string used when opening the service database via the `psycopg2` database API.

A typical value for a single-host deployment would be `"dbname=hatrac"`. In a more complex deployment, this might include remote database server addresses or other connection options.

### `allowed_url_char_class`

A Python RE representing the class of single characters allowed in the
pathname component of a Hatrac URL. The default is a strict
definition: `[-._~A-Za-z0-9/]`. This default combines RFC 3986 and
Hatrac URL parsing rules:

- Non-reserved characters are ASCII alpha-numeric A-Z, a-z, 0-9, and the limited punctuation `-`, `.`, `_`, and `~`.
- Hatrac pathnames can be formed with the non-reserved characters and the `/` path separator, or with percent-encoded UTF-8 sequences.

The purpose of this configuration field is to allow an administrator
to slightly relax the strict rules and allow for additional bare ASCII
characters to be used outside of this specification. It is recommended
that this feature not be used, and instead deployments be updated to
follow the strict quoting rules described above.

### `max_request_payload_size`

An integer byte count. The default is 134217728, i.e. 128 MiB.

This policy setting limits the size of object payload that a client may send to the service in one request. Requests exceeding this size will be rejected with an HTTP `413` error code. To create larger objects, a client must use the chunked upload job feature to send the large object content as a sequence of smaller chunk requests.

### `firewall_acls`

A mapping of predefined ACL names to access control lists. Default configuration:

```
{
  "firewall_acls": {
    "create": ["*"],
    "delete": ["*"],
    "manage_acls": ["*"],
    "manage_metadata": ["*"]
  }
}
```

These predefined ACL names affect the following kinds of request:
- `create`: PUT of namespaces, PUT of objects or new object versions, POST of chunked upload jobs
- `delete`: DELETE of namespaces, objects, and object-versions
- `manage_acls`: PUT or DELETE of ACL sub-resources
- `manage_metadata`: PUT or DELETE of metadata sub-resources

The firewall ACLs are an additional, service-wide authorization step that requests must pass in addition to the fine-grained ACLs configured within the hierarchical namespace. This gives the service operator an option to withdraw some of the self-service privileges that would otherwise be granted to clients who upload content. So, even though a client might be an "owner" of an object or namespace sub-tree, the firewall ACLs might require that they also belong to a special curator group in order to further modify state.

The default is used incrementally to supply any missing firewall ACL in the case that the service configuration sparsely populates the set of ACL names.

### `read_only`

When `true`, changes the default `firewall_acls` ACL content from `["*"]` to `[]`.

This backwards-compatibility feature translates the legacy `read_only` configuration field as a short-hand to supply all firewall ACLs with empty lists, approximating the old feature which blocked all mutation requests with one boolean setting. However, this translation only affects the default ACL value supplied for unconfigured firewall ACL names. In a mixed configuration, the `read_only` option will have no effect on firewall ACLs that are populated in the configuration file.

### `storage_backend`

The name of one of the built-in storage backends for the service. Currently must be one of:

- `"filesystem"`
- `"amazons3"`
- `"overlay"`

Each backend introduces additional backend-specific configuration syntax as well.

### `storage_path` (filesystem backend)

The mounted path where the `"filesystem"` backend reads and writes bulk objects. The default storage path is `"/var/www/hatrac"`.

### `s3_config` (amazons3 backend)

A nested document containing many configuration fields for the amazons3 backend.

```
{
  ...
  "storage_backend": "amazons3",
  "s3_config": {
    "default_session": { ... },
    "buckets": { ... },
    "legacy_mapping": <boolean>,
  }
  ...
}
```

#### `s3_config`.`default_session`

A sub-document passed through as a keyword arguments dictionary for the Python boto3 session constructor, i.e. `boto3.session.Session(**default_session)`. The default when unconfigured is `{}` which uses the built-in default behavior of the API.

This default session will be used to access the S3 API unless a more specific session config is configured for a specific bucket in the `buckets` config described later.

For backwards compatibility, either `default_session` or `session` are recognized as the configuration field name for this concept.

#### `s3_config`.`buckets`

A sub-document mapping one or more sets of bucket-specific configuration to different path prefixes in the Hatrac namespace hierarchy.

```
{
  ...
  "storage_backend": "amazons3",
  "s3_config": {
    "buckets": {
      <prefix>: {
        "bucket_name": <s3 bucket name string>,
        "bucket_path_prefix": <s3 bucket path prefix string>,
        "hatrac_s3_method": <hatrac s3 method name string>,
        "unquote_object_keys": <boolean>,
        "presigned_url_threshold": <integer byte count>,
        "presigned_url_expiration_secs": <integer number of seconds>,
        "session_config": { ... },
        "client_config": { ... }
      }
      ...
    },
  }
  ...
}
```

Each _prefix_ should be a path string such as `"namespace1"` or `"namespace1/namespace2"`. When routing request paths for object access, the *most specific* matching prefix will be found to choose the corresponding _bucket config_ that controls object storage.

Leading and trailing `/` characters will be stripped, so a single configuration for the `""` path prefix is sufficient to configure one bucket for the entire Hatrac namespace.

For backwards compatibility, either `buckets` or `bucket_mappings` are recognized as the configuration field name for this concept.

#### `s3_config`.`legacy_mapping`

When `true`, simplifies the interpretation of the `s3_config`.`bucket` config to ignore deeper path prefixes. The default is `false`.

This is a backwards-compatibility option to emulate the way the legacy codebase would ignore path suffixes and treat `"/prefix/suffix"` the same as `"/prefix"`.

#### `s3_config`.`buckets`.`bucket_name`

The name of the configured bucket in the S3 API.

#### `s3_config`.`buckets`.`bucket_path_prefix`

A path prefix to add to Hatrac storage names when producing S3 object keys. Default `"hatrac"`.

The default is a backwards-compatibility mechanism. It is recommended that new deployments use `""` so that the hierarchical Hatrac namespace maps directly to object prefixes in a dedicated storage bucket.

#### `s3_config`.`buckets`.`hatrac_s3_method`

The name of the desired naming scheme for mapping Hatrac object names to S3 object keys. Default `"pref/**/hname"`.

These names reference specific built-in methods:

- `pref/**/hname`: Object keys combine _bucket path prefix_ and _hierarchical hatrac name_
- `pref/**/hname:hver`: Object keys combine _bucket path prefix_ , _hierarchical hatrac name_ , and hatrac object-version ID.

The set of available methods may be extended in later releases.

#### `s3_config`.`buckets`.`unquote_object_keys`

When `true`, unquote URL-quoting in Hatrac hierarchical object names embedded in S3 storage keys. Default `false`.

Set this `true` for backwards compatibility with buckets written by earlier versions of Hatrac, or if more human-readable object keys are desired. However, it may have undesirable artifacts since an individual namespace or object name fragment in Hatrac might unquote to contain punctuation or Unicode characters.

The new default, `false`, makes object keys less human readable but avoids these potential confusing scenarios.

#### `s3_config`.`buckets`.`presigned_url_threshold`

The smallest object size in bytes that will be served with signed URL redirection. Default `null` disables the feature entirely.

When an integer size greater than `0` is configured, objects smaller than the threshold will be returned immediately by proxying content, while larger objects will be returned indirectly via redirection with signed URLs that allow the client to directly retrieve object content from the S3-compatible object store.

#### `s3_config`.`buckets`.`presigned_url_expiration_secs`

The integer number of seconds that a presigned URL will delegate access privileges to the client. Default `300` (5 minutes).

After the URL expires, the client will need to repeat the Hatrac request to obtain a new signed URL.

#### `s3_config`.`buckets`.`session_config`

A sub-document passed through as a keyword arguments dictionary for the Python boto3 session constructor, i.e. `boto3.session.Session(**session_config)`. The default when unconfigured is to reuse the session from the `s3_config`.`default_session` configuration.

This session will be used to access the S3 API when Hatrac names are routed to this bucket configuration.

#### `s3_config`.`buckets`.`client_config`

A sub-document passed through as a keyword arguments dictionary for the Python boto3 client constructor, i.e. `session.Client(**client_config)`. The default `{}` uses built-in default behavior of the API.

### `error_templates`

A nested JSON document allows customization of HTTP error response content.

The first layer maps specific HTTP error code strings, e.g. `"404"` to a nested document. A special key of `"default"` can designate a generic configuration used for any error code not individually configured.
```
"error_templates": {
  <error code>: {
    ...
  },
  "default": {
    ...
  }
}
```

Each nested document, in turn, maps _lowercase_ HTTP content-types to error templates.
```
"error_templates": {
  <error code>: {
    <content type>: <template>,
    ...
  }
}
```

During error handling, the type of the error response is _negotatied_ by considering the set content-types configured with error templates and the set ofo content-types accepted by the client. If there is a failure to negotiate, the service will choose a default content-type.

### Default error configuration

The built-in default configuration is effectively:
```
"error_templates": {
  "default": {
    "text/plain": "%(message)s",
    "text/html": "<html><body><h1>$(title)s</h1><p>%(message)s</p></body></html>"
  }
}
```

### Error template syntax

The error templates are strings which should use the Python dictionary-interpolation syntax. They are evaluated as `template % dict(...)`. The templates are interpolated with a prepared dictionary of error-specific information:
- `code`: The decimal numeric code of the HTTP error.
- `title`: A short textual label corresponding to the HTTP error code.
- `description`: A longer text description of the error.
- `message`: An alias for the `description` key.


#### Support for legacy error template configurations

The service includes limited backwards-compatibility logic to support an earlier configuration syntax for error templates. A top-level configuration of the form:

```
"<code>_<short type>": <template>
```

will be detected and translated to act as the config:

```
"error_templates": {
  <code>: {
    <type>: <template>
  }
}
```

but *only* for the shortened types `html` and `plain` which are understood as `text/html` and `text/plain`, respectively.

