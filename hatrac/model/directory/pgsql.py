
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

from webauthn2.util import DatabaseConnection, sql_literal, sql_identifier, jsonWriter
import hatrac.core

def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg

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

class HatracName (object):
    """Represent a bound name."""
    _acl_names = []
    _table_name = 'name'

    def __init__(self, directory, **args):
        self.directory = directory
        self.id = args['id']
        self.name = args['name']
        self.is_deleted = args['is_deleted']
        self.acls = dict()
        self._acl_load(**args)

    def __str__(self):
        return self.name

    def _reload(self, db):
        result = self.directory._name_lookup(db, self.name)[0]
        return type(self)(self.directory, **result)

    def _acl_load(self, **args):
        for an in self._acl_names:
            self.acls[an] = set(coalesce(args.get('%s' % an), []))

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
        """Return (nbytes, data_generator) pair for namespace."""
        body = list(self.directory.namespace_enumerate_names(self))
        body = jsonWriter(body) + '\n'
        return (len(body), [body])

class HatracObject (HatracName):
    """Represent a bound object."""
    _acl_names = ['owner', 'create', 'read']

    def __init__(self, directory, **args):
        HatracName.__init__(self, directory, **args)

    def is_object(self):
        return True

    def is_version(self):
        return False

    def create_version_from_file(self, input, nbytes, client_context, content_type=None, content_md5=None):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        return self.directory.create_version_from_file(self, input, nbytes, client_context, content_type, content_md5)

    def get_content(self, client_context):
        """Return (nbytes, data_generator) pair for current version.
        """
        resource = self.get_current_version()
        return resource.get_content(client_context)

    def get_current_version(self):
        """Return HatracObjectVersion instance corresponding to current state.
        """
        return self.directory.get_current_version(self)

    def version_resolve(self, version):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        return self.directory.version_resolve(self, version)

class HatracObjectVersion (HatracName):
    """Represent a bound object version."""
    _acl_names = ['owner', 'read']
    _table_name = 'version'

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.object = object
        self.version = args['version']
        self.content_type = args['content_type']
        self.content_md5 = args['content_md5']

    def __str__(self):
        return '%s:%s' % (self.name, self.version)

    def _reload(self, db):
        object1 = self.object._reload(db)
        result = self.directory._version_lookup(db, object1.id, self.version)[0]
        return type(self)(self.directory, object1, **result)

    def is_object(self):
        return True

    def is_version(self):
        return True

    def get_content(self, client_context):
        return self.directory.get_version_content(self.object, self, client_context)

    def delete(self, client_context):
        """Delete resource and its children."""
        return self.directory.delete_version(self, client_context)


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
            db.query('CREATE SCHEMA hatrac')
            for sql in [
                    _name_table_sql,
                    _version_table_sql
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

            # test ACLs and map out recursive delete
            deleted_versions = []
            deleted_names = []
            resource1.enforce_acl(['owner'], client_context)

            for row in self._namespace_enumerate_versions(db, resource1):
                HatracObjectVersion(self, None, **row).enforce_acl(['owner'], client_context)
                deleted_versions.append( row )

            for row in self._namespace_enumerate_names(db, resource1):
                if row.is_object:
                    HatracObject(self, **row).enforce_acl(['owner'], client_context)
                else:
                    HatracNamespace(self, **row).enforce_acl(['owner'], client_context)
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

    def create_version(self, object, client_context, content_type=None, content_md5=None):
        """Create, persist, and return a HatracObjectVersion instance.

           Newly created instance is marked 'deleted'.
        """
        def db_thunk(db):
            # re-fetch status for ACID test/update
            object1 = object._reload(db)
            if object1.is_deleted:
                raise hatrac.core.NotFound('Object %s is not available.' % object1)
            object1.enforce_acl(['owner', 'create'], client_context)
            result = list(self._create_version(db, object1.id, content_type, content_md5))[0]
            result.name = object.name
            result = HatracObjectVersion(self, object1, **result)
            self._set_resource_acl_role(db, result, 'owner', client_context.client)
            return result

        return self._db_wrapper(db_thunk)

    def create_version_from_file(self, object, input, nbytes, client_context, content_type=None, content_md5=None):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        resource = self.create_version(object, client_context, content_type, content_md5)
        assert resource.is_deleted
        version = self.storage.create_from_file(object.name, input, nbytes, content_type, content_md5)
        self._db_wrapper(lambda db: self._complete_version(db, resource, version))
        return self.version_resolve(object, version)

    def get_version_content(self, object, objversion, client_context):
        """Return (nbytes, data_generator) pair for specific version."""
        def db_thunk(db):
            # re-fetch status for ACID test
            objversion1 = objversion._reload(db)
            if objversion1.is_deleted:
                raise hatrac.core.NotFound('Resource %s is not available.' % objversion1)
            objversion1.enforce_acl(['owner', 'read'], client_context)
            return objversion1

        objversion = self._db_wrapper(db_thunk)
        return self.storage.get_content(object.name, objversion.version)

    def name_resolve(self, name, raise_notfound=True):
        """Return a HatracNamespace or HatracObject instance.
        """
        def db_thunk(db):
            result = list(self._name_lookup(db, name))
            if result:
                result = result[0]
                if raise_notfound and result.is_deleted:
                    raise hatrac.core.NotFound('Resource %s not available.' % name)
                if result.is_object:
                    return HatracObject(self, **result)
                else:
                    return HatracNamespace(self, **result)
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

    def namespace_enumerate_names(self, resource):
        def db_thunk(db):
            resource1 = resource._reload(db)
            return self._namespace_enumerate_names(db, resource1, False)

        return self._db_wrapper(db_thunk)

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

    def _set_resource_acl(self, db, resource, access, acl):
        if access not in resource._acl_names:
            raise hatrac.core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        db.update(
            "hatrac.%s r" % sql_identifier(resource._table_name),
            where="r.id = %s" % sql_literal(resource.id),
            **{access: acl}
        )
        resource.acls[access] = set(acl)

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

    def _create_version(self, db, oid, content_type=None, content_md5=None):
        return db.query("""
INSERT INTO hatrac.version
(nameid, content_type, content_md5, is_deleted)
VALUES (%(nameid)s, %(type)s, %(md5)s, True)
RETURNING *
""" % dict(
    nameid=sql_literal(oid),
    type=content_type and sql_literal(content_type) or 'NULL::text',
    md5=content_md5 and sql_literal(content_md5) or 'NULL::text'
)
        )

    def _complete_version(self, db, resource, version):
        return db.update(
            "hatrac.version",
            where="id = %s" % sql_literal(resource.id),
            is_deleted=False,
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
        
    def _version_list(self, db, nameid, limit=None):
        # TODO: add range keying for scrolling enumeration?
        return db.select(
            ["hatrac.name n", "hatrac.version v"],
            what="v.*, n.name",
            where=' AND '.join([
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
        pattern = "^" + regexp_escape(resource.name) + '/'
        if not recursive:
            pattern += '[^/]+$'
        return db.select(
            ['hatrac.name n'], 
            where=' AND '.join([
                "n.name ~ %s" % sql_literal(pattern),
                "NOT n.is_deleted"
            ])
        )
