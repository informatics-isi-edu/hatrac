
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
        self.is_deleted = args['is_deleted']
        self.acls = ACLs()
        self.acls.directory = directory
        self.acls.resource = self
        self._acl_load(**args)

    @staticmethod
    def construct(directory, **args):
        if args['is_object']:
            return HatracObject(directory, **args)
        else:
            return HatracNamespace(directory, **args)

    def __str__(self):
        return self.name

    def _reload(self, db):
        result = self.directory._name_lookup(db, self.name)[0]
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

class HatracObjectVersion (HatracName):
    """Represent a bound object version."""
    _acl_names = ['owner', 'read']
    _table_name = 'version'

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.object = object
        self.version = args['version']
        self.nbytes = args['nbytes']
        self.content_type = args['content_type']
        self.content_md5 = args['content_md5']

    def __str__(self):
        return '%s:%s' % (self.name, self.version)

    def _reload(self, db):
        object1 = self.object._reload(db)
        result = self.directory._version_lookup(db, object1.id, self.version)
        if not result:
            raise hatrac.core.NotFound("Resource %s not found." % self)
        return type(self)(self.directory, object1, **result[0])

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

class HatracUpload (HatracName):
    """Represent an upload job."""
    def __init__(self, directory, version, **args):
        self.directory = directory
        self.version = version
        self.id = args['id']
        self.versionid = args['versionid']
        self.job = args['job']
        self.chunksize = args['chunksize']

    def __str__(self):
        return "%s;upload/%s" % (str(self.version.object), self.job)

    def _reload(self, db):
        version1 = self.version._reload(db)
        result = self.directory._upload_lookup(db, version1.object.id, self.job)
        if not result:
            raise hatrac.core.NotFound("Resource %s not found." % self)
        result = result[0]
        del result['version']
        return type(self)(self.directory, version1, **result)

    def upload_chunk_from_file(self, position, input, client_context, nbytes, content_md5=None):
        return self.directory.upload_chunk_from_file(self, position, input, client_context, nbytes, content_md5)

    def get_content(self, client_context):
        self.version.enforce_acl(['owner'], client_context)
        body = jsonWriterRaw(dict(
            url=str(self), 
            target=str(self.version.object), 
            owner=self.version.get_acl('owner'),
            chunksize=self.chunksize
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
  is_object bool NOT NULL,
  is_deleted bool NOT NULL,
  owner text[],
  "create" text[],
  read text[]
);

INSERT INTO hatrac.name 
(name, is_object, is_deleted)
VALUES ('/', False, False);
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
CREATE TABLE IF NOT EXISTS hatrac.upload (
  id bigserial PRIMARY KEY,
  versionid int8 NOT NULL REFERENCES hatrac.version(id),
  job text NOT NULL,
  chunksize int8 NOT NULL,
  CHECK(chunksize > 0)
);
"""

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

    def deploy_db(self, admin_roles):
        """Initialize database and set root namespace owners."""
        def db_thunk(db):
            db.query('CREATE SCHEMA IF NOT EXISTS hatrac')
            for sql in [
                    _name_table_sql,
                    _version_table_sql,
                    _upload_table_sql
            ]:
                db.query(sql)

            rootns = HatracNamespace(self, **(self._name_lookup(db, '/')[0]))

            for role in admin_roles:
                self._set_resource_acl_role(db, rootns, 'owner', role)

        self._db_wrapper(db_thunk)

    def create_name(self, name, is_object, client_context):
        """Create, persist, and return a HatracNamespace or HatracObject instance.

        """
        nameparts = [ n for n in name.split('/') if n ]
        parname = "/" + "/".join(nameparts[0:-1])
        relname = nameparts[-1]
        if relname in [ '.', '..' ]:
            raise hatrac.core.BadRequest('Illegal name "%s".' % relname)

        def db_thunk(db): 
            result = list(self._name_lookup(db, name))
            if result:
                raise hatrac.core.Conflict('Name %s already in use.' % name)

            result = list(self._name_lookup(db, parname))
            if not result or result[0].is_deleted:
                raise hatrac.core.Conflict('Parent namespace %s not available.' % parname)
            if result[0].is_object:
                raise hatrac.core.Conflict('Parent %s is not a namespace.' % parname)

            parent = HatracNamespace(self, **result[0])
            parent.enforce_acl(['owner', 'create'], client_context)
            result = self._create_name(db, name, is_object)[0]
            if is_object:
                result = HatracObject(self, **result)
            else:
                result = HatracNamespace(self, **result)

            self._set_resource_acl_role(db, result, 'owner', client_context.client)

        self._db_wrapper(db_thunk)
        
        return self.name_resolve(name)

    def delete_name(self, resource, client_context):
        """Delete an existing namespace or object resource."""
        def db_thunk(db):
            # re-test under transaction
            resource1 = resource._reload(db)
            if resource1.is_deleted:
                raise hatrac.core.NotFound('Namespace %s not available.' % resource)

            if resource.name == '/':
                raise hatrac.core.Forbidden('Root service namespace %s cannot be deleted.' % resource1)

            # test ACLs and map out recursive delete
            deleted_versions = []
            deleted_names = []
            resource1.enforce_acl(['owner'], client_context)

            for row in self._namespace_enumerate_versions(db, resource1):
                HatracObjectVersion(self, None, **row).enforce_acl(['owner'], client_context)
                deleted_versions.append( row )

            for row in self._namespace_enumerate_names(db, resource1):
                HatracName.construct(self, **row).enforce_acl(['owner'], client_context)
                deleted_names.append( row )

            # we only get here if no ACL raised an exception above
            deleted_names.append(resource1)

            for row in deleted_versions:
                self._delete_version(db, row)
            for row in deleted_names:
                self._delete_name(db, row)

            return (deleted_versions, deleted_names)

        # tell storage system to clean up after deletes were committed to DB
        versions, names = self._db_wrapper(db_thunk)
        for row in versions:
            self.storage.delete(row.name, row.version)
        for row in names:
            self.storage.delete_namespace(row.name)

    def delete_version(self, resource, client_context):
        """Delete an existing version."""
        def db_thunk(db):
            # re-test under transaction
            resource1 = resource._reload(db)
            if resource1.is_deleted:
                raise hatrac.core.NotFound('Version %s not available.' % resource)
            resource1.enforce_acl(['owner'], client_context)
            self._delete_version(db, resource1)
            return resource1

        # tell storage system to clean up after deletes were committed to DB
        version = self._db_wrapper(db_thunk)
        self.storage.delete(version.name, version.version)

    def create_version(self, object, client_context, nbytes=None, content_type=None, content_md5=None):
        """Create, persist, and return a HatracObjectVersion instance.

           Newly created instance is marked 'deleted'.
        """
        def db_thunk(db):
            # re-fetch status for ACID test/update
            object1 = object._reload(db)
            if object1.is_deleted:
                raise hatrac.core.NotFound('Object %s is not available.' % object1)
            object1.enforce_acl(['owner', 'create'], client_context)
            result = list(self._create_version(db, object1.id, nbytes, content_type, content_md5))[0]
            result.name = object.name
            result = HatracObjectVersion(self, object1, **result)
            self._set_resource_acl_role(db, result, 'owner', client_context.client)
            return result

        return self._db_wrapper(db_thunk)

    def create_upload(self, version, chunksize, client_context):
        def db_thunk(db):
            result = list(self._create_upload(db, version.id, chunksize))[0]
            return HatracUpload(self, version, **result)

        return self._db_wrapper(db_thunk)

    def create_version_from_file(self, object, input, client_context, nbytes, content_type=None, content_md5=None):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        resource = self.create_version(object, client_context, nbytes, content_type, content_md5)
        assert resource.is_deleted
        version = self.storage.create_from_file(object.name, input, nbytes, content_type, content_md5)
        self._db_wrapper(lambda db: self._complete_version(db, resource, version))
        return self.version_resolve(object, version)

    def create_version_upload_job(self, object, chunksize, client_context, nbytes=None, content_type=None, content_md5=None):
        resource = self.create_version(object, client_context, nbytes, content_type, content_md5)
        assert resource.is_deleted
        resource.version = self.storage.create_upload(object.name, nbytes, content_type, content_md5)
        self._db_wrapper(lambda db: self._complete_version(db, resource, resource.version, True)) # still in deleted state
        return self.create_upload(resource, chunksize, client_context)

    def upload_chunk_from_file(self, upload, position, input, client_context, nbytes, content_md5=None):
        upload.version.enforce_acl(['owner'], client_context)
        if not upload.version.is_deleted:
            raise hatrac.core.Conflict('Further transfers not permitted once upload job is finalized.')
        nchunks = upload.version.nbytes / upload.chunksize
        remainder = upload.version.nbytes % upload.chunksize
        if position < (nchunks - 1) and nbytes != upload.chunksize:
            raise hatrac.core.Conflict('Uploaded chunk byte count %s does not match job chunk size %s.' % (nbytes, upload.chunksize))
        if remainder and position == nchunks and nbytes != remainder:
            raise hatrac.core.Conflict('Uploaded chunk byte count %s does not match final chunk size %s.' % (nbytes, remainder))
        self.storage.upload_chunk_from_file(
            upload.version.object.name, 
            upload.version.version, 
            position, 
            upload.chunksize, 
            input, 
            nbytes, 
            content_md5
        )

    def upload_finalize(self, upload, client_context):
        def db_thunk(db):
            upload1 = upload._reload(db)
            version = upload1.version._reload(db)
            version.enforce_acl(['owner'], client_context)
            self._complete_version(db, version, version.version)
            self._delete_upload(db, upload1)
            return version

        version = self._db_wrapper(db_thunk)
        return self.version_resolve(version.object, version.version)

    def upload_cancel(self, upload, client_context):
        def db_thunk(db):
            upload1 = upload._reload(db)
            version = upload1.version._reload(db)
            version.enforce_acl(['owner'], client_context)
            self._delete_upload(db, upload1)
            return version

        version = self._db_wrapper(db_thunk)
        if version.is_deleted:
            self.storage.delete(version.name, version.version)

    def get_version_content_range(self, object, objversion, get_slice, client_context):
        """Return (nbytes, data_generator) pair for specific version."""
        def db_thunk(db):
            # re-fetch status for ACID test
            objversion1 = objversion._reload(db)
            if objversion1.is_deleted:
                raise hatrac.core.NotFound('Resource %s is not available.' % objversion1)
            objversion1.enforce_acl(['owner', 'read'], client_context)
            return objversion1

        objversion = self._db_wrapper(db_thunk)
        nbytes, content_type, content_md5, data = self.storage.get_content_range(object.name, objversion.version, objversion.content_md5, get_slice)
        # override metadata from directory??
        return (nbytes, objversion.content_type, content_md5, data)

    def get_version_content(self, object, objversion, client_context):
        """Return (nbytes, data_generator) pair for specific version."""
        return self.get_version_content_range(object, objversion, None, client_context)

    def name_resolve(self, name, raise_notfound=True):
        """Return a HatracNamespace or HatracObject instance.
        """
        def db_thunk(db):
            result = list(self._name_lookup(db, name))
            if result:
                result = result[0]
                if raise_notfound and result.is_deleted:
                    raise hatrac.core.NotFound('Resource %s not available.' % name)
                return HatracName.construct(self, **result)
            elif raise_notfound:
                raise hatrac.core.NotFound('Resource %s not found.' % name)
            else:
                return None

        return self._db_wrapper(db_thunk)

    def version_resolve(self, object, version, raise_notfound=True):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        def db_thunk(db):
            result = list(self._version_lookup(db, object.id, version))
            if result:
                result = result[0]
                if raise_notfound and result.is_deleted:
                    raise hatrac.core.NotFound('Resource %s:%s not available.' % (object.name, version))
                return result
            else:
                raise hatrac.core.NotFound("Object version %s:%s not found." % (object.name, version))

        r = self._db_wrapper(db_thunk)
        return HatracObjectVersion(self, object, **r)

    def upload_resolve(self, object, job, raise_notfound=True):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        def db_thunk(db):
            result = list(self._upload_lookup(db, object.id, job))
            if not result:
                raise hatrac.core.NotFound("Upload job %s;upload/%s not found." % (object.name, job))
            return result[0]

        r = self._db_wrapper(db_thunk)
        version = self.version_resolve(object, r.version, False)
        del r['version']
        return HatracUpload(self, version, **r)

    def get_current_version(self, object):
        """Return a HatracObjectVersion instance corresponding to latest.
        """
        def db_thunk(db):
            result = list(self._version_list(db, object.id, limit=1))
            if result:
                return result[0]
            else:
                raise hatrac.core.Conflict('Object %s currently has no content.' % object.name)

        r = self._db_wrapper(db_thunk)
        return HatracObjectVersion(self, object, **r)

    def set_resource_acl_role(self, resource, access, role, client_context):
        def db_thunk(db):
            resource1 = resource._reload(db)
            resource1.enforce_acl(['owner'], client_context)
            self._set_resource_acl_role(db, resource1, access, role)

        return self._db_wrapper(db_thunk)

    def drop_resource_acl_role(self, resource, access, role, client_context):
        def db_thunk(db):
            resource1 = resource._reload(db)
            resource1.enforce_acl(['owner'], client_context)
            if role not in resource1.acls[access]:
                raise hatrac.core.NotFound('%s;acl/%s/%s' % (resource1, access, role))
            self._drop_resource_acl_role(db, resource1, access, role)

        return self._db_wrapper(db_thunk)

    def set_resource_acl(self, resource, access, acl, client_context):
        def db_thunk(db):
            resource1 = resource._reload(db)
            resource1.enforce_acl(['owner'], client_context)
            self._set_resource_acl(db, resource1, access, acl)

        return self._db_wrapper(db_thunk)

    def clear_resource_acl(self, resource, access, client_context):
        def db_thunk(db):
            resource1 = resource._reload(db)
            resource1.enforce_acl(['owner'], client_context)
            self._set_resource_acl(db, resource1, access, [])

        return self._db_wrapper(db_thunk)

    def namespace_enumerate_names(self, resource, recursive=True):
        def db_thunk(db):
            resource1 = resource._reload(db)
            return list(self._namespace_enumerate_names(db, resource1, recursive))

        return [
            HatracName.construct(self, **row)
            for row in self._db_wrapper(db_thunk)
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
(name, is_object, is_deleted)
VALUES (%(name)s, %(isobject)s, False)
RETURNING *
""" % dict(
    name=sql_literal(name),
    isobject=is_object and 'True' or 'False'
)
        )

    def _create_version(self, db, oid, nbytes=None, content_type=None, content_md5=None):
        return db.query("""
INSERT INTO hatrac.version
(nameid, nbytes, content_type, content_md5, is_deleted)
VALUES (%(nameid)s, %(nbytes)s, %(type)s, %(md5)s, True)
RETURNING *
""" % dict(
    nameid=sql_literal(oid),
    nbytes=nbytes is not None and sql_literal(int(nbytes)) or 'NULL::int8',
    type=content_type and sql_literal(content_type) or 'NULL::text',
    md5=content_md5 and sql_literal(content_md5) or 'NULL::text'
)
        )

    def _create_upload(self, db, vid, chunksize):
        jobid = base64.b32encode( 
            (struct.pack('Q', random.getrandbits(64))
             + struct.pack('Q', random.getrandbits(64)))[0:26]
        ).replace('=', '') # strip off '=' padding
        return db.query("""
INSERT INTO hatrac.upload 
(versionid, job, chunksize)
VALUES (%(versionid)s, %(job)s, %(chunksize)s)
RETURNING *
""" % dict(
    versionid=sql_literal(vid),
    chunksize=sql_literal(int(chunksize)),
    job=sql_literal(jobid)
)
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
        return db.delete(
            "hatrac.upload",
            where="id = %s" % sql_literal(resource.id)
        )

    def _name_lookup(self, db, name):
        return db.select(
            ['hatrac.name n'],
            where="n.name = %s" % sql_literal(name)
        )
        
    def _version_lookup(self, db, nameid, version):
        return db.select(
            ["hatrac.name n", "hatrac.version v"],
            what="v.*, n.name",
            where=' AND '.join([
                "v.nameid = n.id",
                "v.nameid = %s" % sql_literal(int(nameid)),
                "v.version = %s" % sql_literal(version)
            ])
        )

    def _upload_lookup(self, db, oid, job):
        return db.select(
            ["hatrac.upload u", "hatrac.version v"],
            what="u.*, v.version",
            where=' AND '.join([
                "v.nameid = %s" % sql_literal(int(oid)),
                "u.versionid = v.id",
                "u.job = %s" % sql_literal(job)
            ])
        )
        
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
        
    def _namespace_enumerate_versions(self, db, resource):
        # return every version under /name... or /name/
        if resource.is_object:
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
