
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

"""Stateful namespace directory using PostgreSQL.

IMPLEMENTATION NOTES:

In this reference implementation, client-visible version identifiers
are completely random, to emphasize that they are not semantically
significant.  Furthermore, an internal version serial ID is used to
track versions so that the client-visible ID can be issued later in
the lifecycle, by the bulk storage driver.

The asynchronous transfer of object versions is handled by temporarily
treating the version as "deleted" until the data is fully stored.  The
lifecycle is thus:

   1. object created (with no versions)
   2. new version created as invisible
      -- unique serial ID
      -- client-visible version NULL
      -- is_deleted True
   3. data transferred to bulk storage
   4. version completed
      -- client-visible version set
      -- is_deleted False

The Hatrac API specification allows arbitrary service-resolved
ordering of concurrent updates, e.g. to decide which version is the
"latest" after a partially ordered update stream.  In this
implementation, concurrent update order is resolved by using the
internal version serial number issued by the database, and always
considering the highest numbered and visible version as the current
version of any particular object.

"""

import web
import base64
import random
import struct
from StringIO import StringIO
from webauthn2.util import DatabaseConnection, sql_literal, sql_identifier, jsonWriterRaw
import hatrac.core
from hatrac.core import coalesce

def regexp_escape(s):
    safe = set(
        [ chr(i) for i in range(ord('a'), ord('z')+1) ]
        + [ chr(i) for i in range(ord('A'), ord('Z')+1) ]
        + [ chr(i) for i in range(ord('0'), ord('9')+1) ]
        + [ '/' ]
    )
               
    def remap(c):
        if c in safe:
            return c
        else:
            return ''.join(('[', c, ']'))
        
    return ''.join([ remap(c) for c in s ])

class ACLEntry (str):
    def get_content(self, client_context):
        self.resource.enforce_acl(['owner'], client_context)
        return len(self), 'text/plain', None, [self]

class ACL (set):
    def get_content(self, client_context):
        self.resource.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(list(self)) + '\n'
        nbytes = len(body)
        return nbytes, 'application/json', None, [body]

    def __getitem__(self, role):
        if role not in self:
            raise hatrac.core.NotFound(
                'ACL member %s;acl/%s/%s not found.' % (self.resource, self.access, role)
            )
        entry = ACLEntry(role + '\n')
        entry.resource = self.resource
        return entry

class ACLs (dict):
    def get_content(self, client_context):
        self.resource.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(self.resource.get_acls()) + '\n'
        nbytes = len(body)
        return nbytes, 'application/json', None, [body]

    def __getitem__(self, k):
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise hatrac.core.BadRequest('Invalid ACL name %s for %s.' % (k, self.resource))

    def __setitem__(self, k, v):
        dict.__setitem__(self, k, v)
        v.resource = self.resource
        v.access = k

class HatracName (object):
    """Represent a bound name."""
    _acl_names = []
    _table_name = 'name'

    def __init__(self, directory, **args):
        self.directory = directory
        self.id = args['id']
        self.name = args['name']
        self.is_deleted = args.get('is_deleted')
        self.acls = ACLs()
        self.acls.directory = directory
        self.acls.resource = self
        self._acl_load(**args)

    @staticmethod
    def construct(directory, **args):
        return [
            HatracNamespace,
            HatracObject
        ][args['subtype']](directory, **args)

    def __str__(self):
        return self.name

    def _reload(self, db, raise_notfound=True):
        result = self.directory._name_lookup(db, self.name, raise_notfound)
        return type(self)(self.directory, **result)

    def _acl_load(self, **args):
        for an in self._acl_names:
            self.acls[an] = ACL(coalesce(args.get('%s' % an), []))

    def get_acl(self, access):
        return list(self.acls[access])

    def get_acls(self):
        return dict([
            (k, self.get_acl(k))
            for k in self.acls
        ])

    def get_uploads(self):
        raise hatrac.core.NotFound('Uploads sub-resource on %s not available.' % self)

    def get_versions(self):
        raise hatrac.core.NotFound('Versions sub-resource on %s not available.' % self)

    def is_object(self):
        raise NotImplementedError()

    def enforce_acl(self, accesses, client_context):
        acl = set()
        for access in accesses:
            acl.update( self.acls[access])
        if '*' in acl \
           or client_context.client and client_context.client in acl \
           or acl.intersection(client_context.attributes):
            return True
        elif client_context.client or client_context.attributes:
            raise hatrac.core.Forbidden('Access to %s forbidden.' % self)
        else:
            raise hatrac.core.Unauthenticated('Authentication required for access to %s' % self)

    def delete(self, client_context):
        """Delete resource and its children."""
        return self.directory.delete_name(self, client_context)

    def set_acl(self, access, acl, client_context):
        self.directory.set_resource_acl(self, access, acl, client_context)

    def clear_acl(self, access, client_context):
        self.directory.clear_resource_acl(self, access, client_context)

    def set_acl_role(self, access, role, client_context):
        self.directory.set_resource_acl_role(self, access, role, client_context)

    def drop_acl_role(self, access, role, client_context):
        self.directory.drop_resource_acl_role(self, access, role, client_context)

class HatracNamespace (HatracName):
    """Represent a bound namespace."""
    _acl_names = ['owner', 'create']

    def __init__(self, directory, **args):
        HatracName.__init__(self, directory, **args)

    def is_object(self):
        return False

    def create_name(self, name, is_object, client_context):
        """Create, persist, and return HatracNamespace or HatracObject instance with given name.

        """
        return self.directory.create_name(name, is_object, client_context)

    def get_content(self, client_context):
        """Return (nbytes, content_type, content_md5, data_generator) for namespace."""
        body = [ str(r) for r in self.directory.namespace_enumerate_names(self, False) ]
        body = jsonWriterRaw(body) + '\n'
        return (len(body), 'application/json', None, body)

class HatracObject (HatracName):
    """Represent a bound object."""
    _acl_names = ['owner', 'create', 'read']

    def __init__(self, directory, **args):
        HatracName.__init__(self, directory, **args)

    def is_object(self):
        return True

    def is_version(self):
        return False

    def get_uploads(self):
        return HatracUploads(self)

    def get_versions(self):
        return HatracVersions(self)

    def create_version_from_file(self, input, client_context, nbytes, content_type=None, content_md5=None):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        return self.directory.create_version_from_file(self, input, client_context, nbytes, content_type, content_md5)

    def create_version_upload_job(self, chunksize, client_context, nbytes=None, content_type=None, content_md5=None):
        return self.directory.create_version_upload_job(self, chunksize, client_context, nbytes, content_type, content_md5)

    def get_content_range(self, client_context, get_slice=None):
        """Return (nbytes, content_type, content_md5, data_generator) for current version.
        """
        resource = self.get_current_version()
        return resource.get_content_range(client_context, get_slice)

    def get_content(self, client_context):
        return self.get_content_range(client_context)

    def get_current_version(self):
        """Return HatracObjectVersion instance corresponding to current state.
        """
        return self.directory.get_current_version(self)

    def version_resolve(self, version):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        return self.directory.version_resolve(self, version)

    def upload_resolve(self, upload):
        return self.directory.upload_resolve(self, upload)

class HatracVersions (object):
    def __init__(self, objresource):
        self.object = objresource

    def get_content(self, client_context):
        self.object.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(self.object.directory.object_enumerate_versions(self.object)) + '\n'
        return len(body), 'application/json', None, [body]

class HatracObjectVersion (HatracName):
    """Represent a bound object version."""
    _acl_names = ['owner', 'read']
    _table_name = 'version'

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.is_deleted = args['is_deleted']
        self.object = object
        self.version = args['version']
        self.nbytes = args['nbytes']
        self.content_type = args['content_type']
        self.content_md5 = args['content_md5']

    def __str__(self):
        return '%s:%s' % (self.name, self.version)

    def _reload(self, db):
        object1 = self.object._reload(db)
        result = self.directory._version_lookup(db, object1, self.version)
        return type(self)(self.directory, object1, **result)

    def is_object(self):
        return True

    def is_version(self):
        return True

    def get_content(self, client_context):
        return self.directory.get_version_content(self.object, self, client_context)

    def get_content_range(self, client_context, get_slice=None):
        return self.directory.get_version_content_range(self.object, self, get_slice, client_context)

    def delete(self, client_context):
        """Delete resource and its children."""
        return self.directory.delete_version(self, client_context)

class HatracUploads (object):
    def __init__(self, objresource):
        self.object = objresource

    def create_version_upload_job(self, *args):
        return self.object.create_version_upload_job(*args)

    def get_content(self, client_context):
        self.object.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(self.object.directory.namespace_enumerate_uploads(self.object)) + '\n'
        return len(body), 'application/json', None, [body]

class HatracUpload (HatracName):
    """Represent an upload job."""
    _acl_names = ['owner']
    _table_name = 'upload'

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.object = object
        self.nameid = args['nameid']
        self.job = args['job']
        self.nbytes = args['nbytes']
        self.chunksize = args['chunksize']
        self.content_type = args['content_type']
        self.content_md5 = args['content_md5']

    def __str__(self):
        return "%s;upload/%s" % (str(self.object), self.job)

    def _reload(self, db):
        object = self.object._reload(db)
        return type(self)(self.directory, object, **self.directory._upload_lookup(db, object, self.job))

    def upload_chunk_from_file(self, position, input, client_context, nbytes, content_md5=None):
        return self.directory.upload_chunk_from_file(self, position, input, client_context, nbytes, content_md5)

    def get_content(self, client_context):
        self.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(dict(
            url=str(self), 
            target=str(self.object), 
            owner=self.get_acl('owner'),
            chunksize=self.chunksize,
            nbytes=self.nbytes,
            content_type=self.content_type,
            content_md5=self.content_md5
            )) + '\n'
        return len(body), 'application/json', None, [body]

    def finalize(self, client_context):
        return self.directory.upload_finalize(self, client_context)

    def cancel(self, client_context):
        return self.directory.upload_cancel(self, client_context)


_name_table_sql = """
CREATE TABLE hatrac.name (
  id bigserial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  subtype int NOT NULL,
  is_deleted bool NOT NULL,
  owner text[],
  "create" text[],
  read text[]
);

INSERT INTO hatrac.name 
(name, subtype, is_deleted)
VALUES ('/', 0, False);
"""

_version_table_sql = """
CREATE TABLE hatrac.version (
  id bigserial PRIMARY KEY,
  nameid int8 NOT NULL REFERENCES hatrac.name(id),
  version text,
  nbytes int8,
  content_type text,
  content_md5 text,
  is_deleted bool NOT NULL,
  owner text[],
  read text[],
  UNIQUE(nameid, version),
  CHECK(version IS NOT NULL OR is_deleted)
);

CREATE INDEX version_nameid_id_idx ON hatrac.version (nameid, id);
"""

_upload_table_sql = """
CREATE TABLE hatrac.upload (
  id bigserial PRIMARY KEY,
  nameid int8 NOT NULL REFERENCES hatrac.name(id),
  job text NOT NULL,
  nbytes int8 NOT NULL,
  chunksize int8 NOT NULL,
  content_type text,
  content_md5 text,
  owner text[],
  UNIQUE(nameid, job),
  CHECK(chunksize > 0)
);
"""

_chunk_table_sql = """
CREATE TABLE hatrac.chunk (
  uploadid int8 NOT NULL REFERENCES hatrac.upload(id),
  position int8 NOT NULL,
  aux json,
  UNIQUE(uploadid, position)
);
"""

def db_wrap(reload_pos=None, transform=None, enforce_acl=None):
    """Decorate a HatracDirectory method whose body should run in self._db_wrapper().

       If reload_pos is not None: 
          replace args[reload_pos] with args[reload_pos]._reload(db)

       If enforce_acl is (rpos, cpos, acls):
          call args[rpos].enforce_acl(acls, args[cpos])

       If transform is not None: return transform(result)
    """
    def helper(original_method):
        def wrapper(*args):
            def db_thunk(db):
                args1 = list(args)
                if reload_pos is not None:
                    args1[reload_pos] = args1[reload_pos]._reload(db)
                if enforce_acl is not None:
                    rpos, cpos, acls = enforce_acl
                    args1[rpos].enforce_acl(acls, args[cpos])
                result = original_method(*args1, db=db)
                if transform:
                    result = transform(result)
                return result
            return args[0]._db_wrapper(db_thunk)
        return wrapper
    return helper

class HatracDirectory (DatabaseConnection):
    """Stateful Hatrac Directory tracks bound names and object versions.

    """
    def __init__(self, config, storage):
        DatabaseConnection.__init__(
            self, 
            config,
            extended_exceptions=[
                (hatrac.core.HatracException, False, False)
            ]
        )
        self.storage = storage

    @db_wrap()
    def deploy_db(self, admin_roles, db=None):
        """Initialize database and set root namespace owners."""
        db.query('CREATE SCHEMA IF NOT EXISTS hatrac')
        for sql in [
                _name_table_sql,
                _version_table_sql,
                _upload_table_sql,
                _chunk_table_sql
        ]:
            db.query(sql)

        rootns = HatracNamespace(self, **(self._name_lookup(db, '/')))

        for role in admin_roles:
            self._set_resource_acl_role(db, rootns, 'owner', role)

    @db_wrap()
    def create_name(self, name, is_object, client_context, db=None):
        """Create, persist, and return a HatracNamespace or HatracObject instance.

        """
        nameparts = [ n for n in name.split('/') if n ]
        parname = "/" + "/".join(nameparts[0:-1])
        relname = nameparts[-1]
        if relname in [ '.', '..' ]:
            raise hatrac.core.BadRequest('Illegal name "%s".' % relname)

        try:
            self._name_lookup(db, name)
            raise hatrac.core.Conflict('Name %s already in use.' % name)
        except hatrac.core.NotFound, ev:
            pass
            
        parent = HatracName.construct(self, **self._name_lookup(db, parname))
        if parent.is_object():
            raise hatrac.core.Conflict('Parent %s is not a namespace.' % parname)

        parent.enforce_acl(['owner', 'create'], client_context)
        resource = HatracName.construct(self, **self._create_name(db, name, is_object))
        self._set_resource_acl_role(db, resource, 'owner', client_context.client)
        return resource

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner']), transform=lambda thunk: thunk())
    def delete_name(self, resource, client_context, db=None):
        """Delete an existing namespace or object resource."""
        if resource.name == '/':
            raise hatrac.core.Forbidden('Root service namespace %s cannot be deleted.' % resource)

        # test ACLs and map out recursive delete
        deleted_versions = []
        deleted_names = []

        for row in self._namespace_enumerate_versions(db, resource):
            HatracObjectVersion(self, None, **row).enforce_acl(['owner'], client_context)
            deleted_versions.append( row )

        for row in self._namespace_enumerate_names(db, resource):
            HatracName.construct(self, **row).enforce_acl(['owner'], client_context)
            deleted_names.append( row )

        # we only get here if no ACL raised an exception above
        deleted_names.append(resource)

        for row in deleted_versions:
            self._delete_version(db, row)
        for row in deleted_names:
            self._delete_name(db, row)

        def cleanup():
            # tell storage system to clean up after deletes were committed to DB
            for row in deleted_versions:
                self.storage.delete(row.name, row.version)
            for row in deleted_names:
                self.storage.delete_namespace(row.name)

        return cleanup

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner']), transform=lambda thunk: thunk())
    def delete_version(self, resource, client_context, db=None):
        """Delete an existing version."""
        self._delete_version(db, resource)
        return lambda : self.storage.delete(resource.name, resource.version)

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner', 'create']))
    def create_version(self, object, client_context, nbytes=None, content_type=None, content_md5=None, db=None):
        """Create, persist, and return a HatracObjectVersion instance.

           Newly created instance is marked 'deleted'.
        """
        resource = HatracObjectVersion(self, object, **self._create_version(db, object, nbytes, content_type, content_md5))
        self._set_resource_acl_role(db, resource, 'owner', client_context.client)
        return resource

    def create_version_from_file(self, object, input, client_context, nbytes, content_type=None, content_md5=None):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        resource = self.create_version(object, client_context, nbytes, content_type, content_md5)
        version = self.storage.create_from_file(object.name, input, nbytes, content_type, content_md5)
        self._db_wrapper(lambda db: self._complete_version(db, resource, version))
        return self.version_resolve(object, version)

    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner']))
    def create_version_upload_job(self, object, chunksize, client_context, nbytes=None, content_type=None, content_md5=None, db=None):
        job = self.storage.create_upload(object.name, nbytes, content_type, content_md5)
        resource = HatracUpload(self, object, **self._create_upload(db, object, job, chunksize, nbytes, content_type, content_md5))
        self._set_resource_acl_role(db, resource, 'owner', client_context.client)
        return resource

    def upload_chunk_from_file(self, upload, position, input, client_context, nbytes, content_md5=None):
        upload.enforce_acl(['owner'], client_context)
        nchunks = upload.nbytes / upload.chunksize
        remainder = upload.nbytes % upload.chunksize
        if position < (nchunks - 1) and nbytes != upload.chunksize:
            raise hatrac.core.Conflict('Uploaded chunk byte count %s does not match job chunk size %s.' % (nbytes, upload.chunksize))
        if remainder and position == nchunks and nbytes != remainder:
            raise hatrac.core.Conflict('Uploaded chunk byte count %s does not match final chunk size %s.' % (nbytes, remainder))
        aux = self.storage.upload_chunk_from_file(
            upload.object.name, 
            upload.job, 
            position, 
            upload.chunksize, 
            input, 
            nbytes, 
            content_md5
        )

        def db_thunk(db):
            self._track_chunk(db, upload, position, aux)

        if self.storage.track_chunks:
            self._db_wrapper(db_thunk)

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner']))
    def upload_finalize(self, upload, client_context, db=None):
        if self.storage.track_chunks:
            chunk_aux = list(self._chunk_list(db, upload))
        else:
            chunk_aux = None
        version_id = self.storage.finalize_upload(upload.name, upload.job, chunk_aux)
        version = HatracObjectVersion(self, upload.object, **self._create_version(db, upload.object, upload.nbytes, upload.content_type, upload.content_md5))
        self._set_resource_acl_role(db, version, 'owner', client_context.client)
        self._complete_version(db, version, version_id)
        self._delete_upload(db, upload)
        version.version = version_id
        return version

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner']), transform=lambda thunk: thunk())
    def upload_cancel(self, upload, client_context, db=None):
        self._delete_upload(db, upload)
        return lambda : self.storage.cancel_upload(upload.name, upload.job)

    @db_wrap(reload_pos=2, enforce_acl=(2, 4, ['owner', 'read']))
    def get_version_content_range(self, object, objversion, get_slice, client_context, db=None):
        """Return (nbytes, data_generator) pair for specific version."""
        if objversion.is_deleted:
            raise hatrac.core.NotFound('Resource %s is not available.' % objversion)
        nbytes, content_type, content_md5, data = self.storage.get_content_range(object.name, objversion.version, objversion.content_md5, get_slice)
        return nbytes, objversion.content_type, content_md5, data

    def get_version_content(self, object, objversion, client_context):
        """Return (nbytes, data_generator) pair for specific version."""
        return self.get_version_content_range(object, objversion, None, client_context)

    @db_wrap()
    def name_resolve(self, name, raise_notfound=True, db=None):
        """Return a HatracNamespace or HatracObject instance.
        """
        try:
            return HatracName.construct(self, **self._name_lookup(db, name))
        except hatrac.core.NotFound, ev:
            if raise_notfound:
                raise ev

    @db_wrap()
    def version_resolve(self, object, version, raise_notfound=True, db=None):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        return HatracObjectVersion(
            self, 
            object, 
            **self._version_lookup(db, object, version, not raise_notfound)
        )

    @db_wrap(reload_pos=1)
    def upload_resolve(self, object, job, raise_notfound=True, db=None):
        """Return a HatracUpload instance corresponding to referenced job.
        """
        return HatracUpload(self, object, **self._upload_lookup(db, object, job))

    @db_wrap(reload_pos=1)
    def get_current_version(self, object, db=None):
        """Return a HatracObjectVersion instance corresponding to latest.
        """
        results = self._version_list(db, object.id, limit=1)
        if results:
            return HatracObjectVersion(self, object, **results[0])
        else:
            raise hatrac.core.Conflict('Object %s currently has no content.' % object.name)

    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner']))
    def set_resource_acl_role(self, resource, access, role, client_context, db=None):
        self._set_resource_acl_role(db, resource, access, role)

    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner']))
    def drop_resource_acl_role(self, resource, access, role, client_context, db=None):
        if role not in resource.acls[access]:
            raise hatrac.core.NotFound('Resource %s;acl/%s/%s not found.' % (resource, access, role))
        self._drop_resource_acl_role(db, resource, access, role)

    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner']))
    def set_resource_acl(self, resource, access, acl, client_context, db=None):
        self._set_resource_acl(db, resource, access, acl)

    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner']))
    def clear_resource_acl(self, resource, access, client_context, db=None):
        self._set_resource_acl(db, resource, access, [])

    @db_wrap(reload_pos=1)
    def object_enumerate_versions(self, object, db=None):
        """Return a list of versions
        """
        return [
            '%s:%s' % (r.name, r.version)
            for r in self._version_list(db, object.id) 
        ]

    @db_wrap(reload_pos=1)
    def namespace_enumerate_names(self, resource, recursive=True, db=None):
        return [
            HatracName.construct(self, **row)
            for row in self._namespace_enumerate_names(db, resource, recursive)
        ]

    @db_wrap(reload_pos=1)
    def namespace_enumerate_uploads(self, resource, recursive=True, db=None):
        return [
            '%s;upload/%s' % (r.name, r.job)
            for r in self._namespace_enumerate_uploads(db, resource, recursive) 
        ]

    def _set_resource_acl_role(self, db, resource, access, role):
        if access not in resource._acl_names:
            raise hatrac.core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        # need to use raw SQL to compute modified array in database
        db.query("""
UPDATE hatrac.%(table)s n
SET %(acl)s = array_append(coalesce(n.%(acl)s, ARRAY[]::text[]), %(role)s)
WHERE n.id = %(id)s
  AND NOT coalesce(ARRAY[%(role)s] && n.%(acl)s, False);
""" % dict(
    table=sql_identifier(resource._table_name),
    acl=sql_identifier(access),
    id=sql_literal(resource.id),
    role=sql_literal(role)
)
        )
        resource.acls[access].add(role)

    def _drop_resource_acl_role(self, db, resource, access, role):
        if access not in resource._acl_names:
            raise hatrac.core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        # need to use raw SQL to compute modified array in database
        db.query("""
UPDATE hatrac.%(table)s n
SET %(acl)s = array_remove(coalesce(n.%(acl)s, ARRAY[]::text[]), %(role)s)
WHERE n.id = %(id)s
  AND coalesce(ARRAY[%(role)s] && n.%(acl)s, False);
""" % dict(
    table=sql_identifier(resource._table_name),
    acl=sql_identifier(access),
    id=sql_literal(resource.id),
    role=sql_literal(role)
)
        )
        resource.acls[access].add(role)

    def _set_resource_acl(self, db, resource, access, acl):
        if access not in resource._acl_names:
            raise hatrac.core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        db.update(
            "hatrac.%s r" % sql_identifier(resource._table_name),
            where="r.id = %s" % sql_literal(resource.id),
            **{access: acl}
        )
        resource.acls[access] = ACL(acl)

    def _create_name(self, db, name, is_object=False):
        return db.query("""
INSERT INTO hatrac.name
(name, subtype, is_deleted)
VALUES (%(name)s, %(isobject)s, False)
RETURNING *
""" % dict(
    name=sql_literal(name),
    isobject=is_object and 1 or 0
)
        )[0]

    def _create_version(self, db, object, nbytes=None, content_type=None, content_md5=None):
        return db.query("""
INSERT INTO hatrac.version
(nameid, nbytes, content_type, content_md5, is_deleted)
VALUES (%(nameid)s, %(nbytes)s, %(type)s, %(md5)s, True)
RETURNING *, %(name)s AS "name"
""" % dict(
    name=sql_literal(object.name),
    nameid=sql_literal(object.id),
    nbytes=nbytes is not None and sql_literal(int(nbytes)) or 'NULL::int8',
    type=content_type and sql_literal(content_type) or 'NULL::text',
    md5=content_md5 and sql_literal(content_md5) or 'NULL::text'
)
        )[0]

    def _create_upload(self, db, object, job, chunksize, nbytes, content_type, content_md5):
        return db.query("""
INSERT INTO hatrac.upload 
(nameid, job, nbytes, chunksize, content_type, content_md5)
VALUES (%(nameid)s, %(job)s, %(nbytes)s, %(chunksize)s, %(content_type)s, %(content_md5)s)
RETURNING *, %(name)s AS "name"
""" % dict(
    name=sql_literal(object.name),
    nameid=sql_literal(object.id),
    job=sql_literal(job),
    nbytes=sql_literal(int(nbytes)),
    chunksize=sql_literal(int(chunksize)),
    content_type=sql_literal(content_type),
    content_md5=sql_literal(content_md5)
)
        )[0]

    def _track_chunk(self, db, upload, position, aux):
        sql_fields = dict(
            uploadid=sql_literal(upload.id),
            position=sql_literal(int(position)),
            aux=sql_literal(jsonWriterRaw(aux))
        )
        
        try:
            result = self._chunk_lookup(db, upload, position)
            return db.query("""
UPDATE hatrac.chunk
SET aux = %(aux)s
WHERE uploadid = %(uploadid)s AND position = %(position)s
""" % sql_fields
            )
            
        except hatrac.core.NotFound:
            return db.query("""
INSERT INTO hatrac.chunk
(uploadid, position, aux)
VALUES (%(uploadid)s, %(position)s, %(aux)s)
""" % sql_fields
            )

    def _complete_version(self, db, resource, version, is_deleted=False):
        return db.update(
            "hatrac.version",
            where="id = %s" % sql_literal(resource.id),
            is_deleted=is_deleted,
            version=version
        )

    def _delete_name(self, db, resource):
        return db.update(
            "hatrac.name",
            where="id = %s" % sql_literal(resource.id),
            is_deleted=True
        )

    def _delete_version(self, db, resource):
        return db.update(
            "hatrac.version",
            where="id = %s" % sql_literal(resource.id),
            is_deleted=True
        )

    def _delete_upload(self, db, resource):
        db.delete(
            "hatrac.chunk",
            where="uploadid = %s" % sql_literal(resource.id)
        )
        return db.delete(
            "hatrac.upload",
            where="id = %s" % sql_literal(resource.id)
        )

    def _name_lookup(self, db, name, check_deleted=True):
        wheres = ["n.name = %s" % sql_literal(name)]
        if check_deleted:
            wheres.append("NOT n.is_deleted")
        results = db.select(
            ['hatrac.name n'],
            where=' AND '.join(wheres)
        )
        if not results:
            raise hatrac.core.NotFound('Resource %s not found.' % name)
        return results[0]
        
    def _version_lookup(self, db, object, version, allow_deleted=True):
        result = db.select(
            ["hatrac.name n", "hatrac.version v"],
            what="v.*, n.name",
            where=' AND '.join([
                "v.nameid = n.id",
                "v.nameid = %s" % sql_literal(int(object.id)),
                "v.version = %s" % sql_literal(version)
            ])
        )
        if not result:
            raise hatrac.core.NotFound("Resource %s:%s not found." % (object, version))
        result = result[0]
        if result.is_deleted and not allow_deleted:
            raise hatrac.core.NotFound("Resource %s:%s not available." % (object, version))
        return result

    def _upload_lookup(self, db, object, job):
        result = db.select(
            ["hatrac.upload u", "hatrac.name n"],
            what="u.*, n.name",
            where=' AND '.join([
                "u.nameid = %s" % sql_literal(int(object.id)),
                "u.nameid = n.id",
                "u.job = %s" % sql_literal(job)
            ])
        )
        if not result:
            raise hatrac.core.NotFound("Resource %s;upload/%s not found." % (object, job))
        return result[0]
        
    def _chunk_lookup(self, db, upload, position):
        result = self._chunk_list(db, upload, position)
        if not result:
            raise hatrac.core.NotFound("Resource %s/%s not found." % (upload, position))
        return result[0]
        
    def _version_list(self, db, nameid, limit=None):
        # TODO: add range keying for scrolling enumeration?
        return db.select(
            ["hatrac.name n", "hatrac.version v"],
            what="v.*, n.name",
            where=' AND '.join([
                "v.nameid = %d" % nameid,
                "v.nameid = n.id",
                "NOT v.is_deleted"
            ]),
            order="v.id DESC",
            limit=limit
        )
        
    def _chunk_list(self, db, upload, position=None):
        wheres = [ "uploadid = %s" % sql_literal(int(upload.id)) ]
        if position is not None:
            wheres.append( "position = %s" % sql_literal(int(position)) )
        result = db.select(
            ["hatrac.chunk"],
            what="*",
            where=' AND '.join(wheres)
        )
        if not result:
            raise hatrac.core.NotFound("Chunk data %s/%s not found." % (upload, position))
        return result

    def _namespace_enumerate_versions(self, db, resource):
        # return every version under /name... or /name/
        if resource.is_object():
            pattern = 'n.name = %s' % sql_literal(resource.name)
        else:
            pattern = "n.name ~ %s" % sql_literal("^" + regexp_escape(resource.name) + '/')
        return db.select(
            ['hatrac.name n', 'hatrac.version v'], 
            where=' AND '.join([
                "v.nameid = n.id",
                pattern,
                "NOT v.is_deleted"
            ])
        )
        
    def _namespace_enumerate_names(self, db, resource, recursive=True):
        # return every namespace or object under /name/...
        pattern = "^" + regexp_escape(resource.name)
        if pattern[-1] != '/':
            # TODO: fix special cases for rootns?
            pattern += '/'
        if not recursive:
            pattern += '[^/]+$'
        return db.select(
            ['hatrac.name n'], 
            where=' AND '.join([
                "n.name ~ %s" % sql_literal(pattern),
                "NOT n.is_deleted"
            ])
        )
        
    def _namespace_enumerate_uploads(self, db, resource, recursive=True):
        # return every upload under /name... or /name/
        if resource.is_object():
            pattern = 'n.name = %s' % sql_literal(resource.name)
        else:
            pattern = "n.name ~ %s" % sql_literal("^" + regexp_escape(resource.name) + '/')
        return db.select(
            ['hatrac.name n', 'hatrac.upload u'], 
            what="u.*, n.name",
            where=' AND '.join([
                "u.nameid = n.id",
                pattern
            ])
        )
        
