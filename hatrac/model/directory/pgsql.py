
#
# Copyright 2015-2019 University of Southern California
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

import os
import sys
import web
import urllib
import binascii
import base64
import random
import struct
import datetime
import psycopg2
import psycopg2.pool
from psycopg2.extras import DictCursor

from webauthn2.util import jsonWriter, negotiated_content_type

from ...core import coalesce, Metadata, sql_literal, sql_identifier
from ... import core

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
    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = self + '\n'
        return len(body), Metadata({'content-type': 'text/plain'}), body

class ACL (set):
    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = jsonWriter(list(self)) + b'\n'
        return len(body), Metadata({'content-type': 'application/json'}), body

    def __getitem__(self, role):
        if role not in self:
            raise core.NotFound(
                'ACL member %s;acl/%s/%s not found.' % (self.resource, self.access, role)
            )
        entry = ACLEntry(role + '\n')
        entry.resource = self.resource
        return entry

class ACLs (dict):
    def is_object(self):
        return False

    def get_content(self, client_context, get_data=True):
        self.resource.enforce_acl(['owner', 'ancestor_owner'], client_context)
        body = jsonWriter(self.resource.get_acls()) + b'\n'
        nbytes = len(body)
        return nbytes, Metadata({'content-type': 'application/json'}), body

    def __getitem__(self, k):
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise core.BadRequest('Invalid ACL name %s for %s.' % (k, self.resource))

    def __setitem__(self, k, v):
        dict.__setitem__(self, k, v)
        v.resource = self.resource
        v.access = k

def negotiated_uri_list(parent, resources, metadata={}):
    """Returns nbytes, Metadata, body"""
    metadata = dict(metadata)
    metadata['content-type'] = negotiated_content_type(
        ['application/json', 'text/uri-list', 'text/html'],
        'application/json'
    )
    uris = sorted([r.asurl() for r in resources])
    if metadata['content-type'] == 'text/uri-list':
        body = '\n'.join(uris) + '\n'
    elif metadata['content-type'] == 'text/html':
        body = "<!DOCTYPE html>\n<html>\n  <h1>Index of {parent}</h1>\n{children}\n</html>".format(
            parent=parent.asurl(),
            children='<br/>\n'.join(['  <a href="%s">%s</a>' % (uri, os.path.basename(uri)) for uri in uris])
        )
    else:
        body = jsonWriter(uris) + b'\n'
        metadata['content-type'] = 'application/json'
    return len(body), Metadata(metadata), body

class HatracName (object):
    """Represent a bound name."""
    _acl_names = []
    _ancestor_acl_names = []
    _table_name = 'name'

    def __init__(self, directory, **args):
        self.directory = directory
        self.id = args['id']
        self.ancestors = args['ancestors']
        self.pid = args['pid']
        self.name = args['name']
        self.is_deleted = args.get('is_deleted')
        self.metadata = Metadata(args.get('metadata', {}))
        self.metadata.resource = self
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
        return self.directory.prefix + self.name

    def asurl(self):
        return self.directory.prefix + '/'.join(map(lambda s: urllib.parse.quote(s, ''), self.name.split('/')))

    def _reload(self, conn, cur, raise_notfound=True):
        result = self.directory._name_lookup(conn, cur, self.name, raise_notfound)
        return type(self)(self.directory, **result)

    def _acl_load(self, **args):
        for an in self._acl_names:
            self.acls[an] = ACL(coalesce(args.get('%s' % an), []))
        for an in self._ancestor_acl_names:
            self.acls[an] = ACL(coalesce(args.get('%s' % an), []))

    def get_acl(self, access):
        return list(self.acls[access])

    def get_acls(self):
        return dict([
            (k, self.get_acl(k))
            for k in self.acls if k in self._acl_names
        ])

    def get_uploads(self):
        raise core.NotFound('Uploads sub-resource on %s not available.' % self)

    def get_versions(self):
        raise core.NotFound('Versions sub-resource on %s not available.' % self)

    def is_object(self):
        raise NotImplementedError()

    def enforce_acl(self, accesses, client_context):
        acl = set()
        for access in accesses:
            acl.update( self.acls.get(access, ACL()))
        client = client_context.client or None
        client = client['id'] if type(client) is dict else client
        attributes = set([
            attr['id'] if type(attr) is dict else attr
            for attr in client_context.attributes
        ])
        if '*' in acl \
           or client in acl \
           or acl.intersection(attributes):
            return True
        elif client_context.client is not None:
            raise core.Forbidden('Access to %s forbidden.' % self)
        else:
            raise core.Unauthenticated('Authentication required for access to %s' % self)

    def delete(self, client_context):
        """Delete resource and its children."""
        return self.directory.delete_name(self, client_context)

    def update_metadata(self, updates, client_context):
        self.directory.update_resource_metadata(self, updates, client_context)

    def pop_metadata(self, fieldname, client_context):
        self.directory.pop_resource_metadata(self, fieldname, client_context)
        
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
    _acl_names = ['owner', 'create', 'subtree-owner', 'subtree-create', 'subtree-read', 'subtree-update']
    _ancestor_acl_names = ['ancestor_owner', 'ancestor_create']

    def __init__(self, directory, **args):
        HatracName.__init__(self, directory, **args)

    def is_object(self):
        return False

    def create_name(self, name, is_object, client_context):
        """Create, persist, and return HatracNamespace or HatracObject instance with given name.

        """
        return self.directory.create_name(name, is_object, client_context)

    def get_content(self, client_context, get_data=True):
        """Return (nbytes, metadata, data_generator) for namespace."""
        return negotiated_uri_list(self, self.directory.namespace_enumerate_names(self, False))

class HatracObject (HatracName):
    """Represent a bound object."""
    _acl_names = ['owner', 'update', 'read', 'subtree-owner', 'subtree-read']
    _ancestor_acl_names = ['ancestor_owner', 'ancestor_update']

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

    def create_version_from_file(self, input, client_context, nbytes, metadata={}):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        return self.directory.create_version_from_file(self, input, client_context, nbytes, metadata)

    def create_version_upload_job(self, chunksize, client_context, nbytes=None, metadata={}):
        return self.directory.create_version_upload_job(self, chunksize, client_context, nbytes, metadata)

    def get_content_range(self, client_context, get_slice=None, get_data=True):
        """Return (nbytes, metadata, data_generator) for current version.
        """
        resource = self.get_current_version()
        return resource.get_content_range(client_context, get_slice, get_data=get_data)

    def get_content(self, client_context, get_data=True):
        return self.get_content_range(client_context, get_data=get_data)

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
    def is_object(self):
        return False

    def __init__(self, objresource):
        self.object = objresource

    def asurl(self):
        return self.object.asurl()

    def get_content(self, client_context, get_data=True):
        self.object.enforce_acl(['owner', 'ancestor_owner'], client_context)
        return negotiated_uri_list(self, self.object.directory.object_enumerate_versions(self.object))

class HatracObjectVersion (HatracName):
    """Represent a bound object version."""
    _acl_names = ['owner', 'read']
    # we pull in the object's subtree-* ACLs and treat them like ancestor ACLs
    # since ancestor ACLs only roll up the ancestor namespaces but not the object itself
    _ancestor_acl_names = ['ancestor_owner', 'ancestor_read', 'subtree-owner', 'subtree-read']
    _table_name = 'version'

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.is_deleted = args['is_deleted']
        self.object = object
        self.version = args['version']
        self.nbytes = args['nbytes']

    def __str__(self):
        return '%s:%s' % (self.object, self.version)

    def asurl(self):
        return '%s:%s' % (self.object.asurl(), urllib.parse.quote(self.version, ''))

    def _reload(self, conn, cur):
        object1 = self.object._reload(conn, cur)
        result = self.directory._version_lookup(conn, cur, object1, self.version)
        return type(self)(self.directory, object1, **result)

    def is_object(self):
        return True

    def is_version(self):
        return True

    def get_content(self, client_context, get_data=True):
        return self.directory.get_version_content(self.object, self, client_context, get_data)

    def get_content_range(self, client_context, get_slice=None, get_data=True):
        return self.directory.get_version_content_range(self.object, self, get_slice, client_context, get_data)

    def delete(self, client_context):
        """Delete resource and its children."""
        return self.directory.delete_version(self, client_context)

class HatracUploads (object):
    def is_object(self):
        return False

    def __init__(self, objresource):
        self.object = objresource

    def asurl(self):
        return self.object.asurl()

    def create_version_upload_job(self, *args):
        return self.object.create_version_upload_job(*args)

    def get_content(self, client_context, get_data=True):
        self.object.enforce_acl(['owner'], client_context)
        return negotiated_uri_list(self, self.object.directory.namespace_enumerate_uploads(self.object))

class HatracUpload (HatracName):
    """Represent an upload job."""
    _acl_names = ['owner']
    _ancestor_acl_names = ['ancestor_owner']
    _table_name = 'upload'

    def is_object(self):
        return False

    def __init__(self, directory, object, **args):
        HatracName.__init__(self, directory, **args)
        self.object = object
        self.nameid = args['nameid']
        self.job = args['job']
        self.nbytes = args['nbytes']
        self.chunksize = args['chunksize']

    def __str__(self):
        return "%s;upload/%s" % (self.object, self.job)

    def asurl(self):
        return '%s;upload/%s' % (self.object.asurl(), urllib.parse.quote(self.job, ''))

    def _reload(self, conn, cur):
        object = self.object._reload(conn, cur)
        return type(self)(self.directory, object, **self.directory._upload_lookup(conn, cur, object, self.job))

    def upload_chunk_from_file(self, position, input, client_context, nbytes, metadata={}):
        return self.directory.upload_chunk_from_file(self, position, input, client_context, nbytes, metadata)

    def get_content(self, client_context, get_data=True):
        self.enforce_acl(['owner'], client_context)
        metadata = self.metadata.to_http()
        body = {
            'url': str(self), 
            'target': str(self.object), 
            'owner': self.get_acl('owner'),
            'chunk-length': self.chunksize,
            'content-length': self.nbytes
        }
        for hdr in {
                'content-type',
                'content-md5',
                'content-sha256',
                'content-disposition',
        }:
            if hdr in metadata:
                body[hdr] = metadata[hdr]
        body = jsonWriter(body) + b'\n'
        return len(body), Metadata({'content-type': 'application/json'}), body

    def finalize(self, client_context):
        return self.directory.upload_finalize(self, client_context)

    def cancel(self, client_context):
        return self.directory.upload_cancel(self, client_context)

class connection (psycopg2.extensions.connection):
    """Customized psycopg2 connection factory

    """
    def __init__(self, dsn):
        psycopg2.extensions.connection.__init__(self, dsn)
        try:
            self._prepare_hatrac_stmts()
        except psycopg2.ProgrammingError:
            self.rollback()

    def _prepare_hatrac_stmts(self):
        cur = self.cursor()
        cur.execute("""
        
        DEALLOCATE PREPARE ALL;

        PREPARE hatrac_complete_version (int8, text, boolean) AS 
          UPDATE hatrac.version  SET is_deleted = $3, version = $2  WHERE id = $1 ;

        PREPARE hatrac_delete_version (int8) AS
          UPDATE hatrac.version  SET is_deleted = True  WHERE id = $1 ;

        PREPARE hatrac_delete_name (int8) AS
          UPDATE hatrac.name  SET is_deleted = True  WHERE id = $1 ;

        PREPARE hatrac_delete_chunks (int8) AS
          DELETE FROM hatrac.chunk WHERE uploadid = $1 ;

        PREPARE hatrac_delete_upload (int8) AS
          DELETE FROM hatrac.upload WHERE id = $1 ;
        
        PREPARE hatrac_name_lookup (text, boolean) AS
          SELECT n.*, %(owner_acl)s, %(update_acl)s, %(read_acl)s, %(create_acl)s
          FROM hatrac.name n
          WHERE n.name = $1 AND (NOT n.is_deleted OR NOT $2) ;

        PREPARE hatrac_version_lookup (int8, text) AS
          SELECT v.*, n.name, n.pid, n.ancestors, %(owner_acl)s, %(read_acl)s
          FROM hatrac.version v
          JOIN hatrac.name n ON (v.nameid = n.id)
          WHERE v.nameid = $1 AND v.version = $2 ;

        PREPARE hatrac_upload_lookup(int8, text) AS
          SELECT u.*, n.name, n.pid, n.ancestors, %(owner_acl)s
          FROM hatrac.upload u
          JOIN hatrac.name n ON (u.nameid = n.id)
          WHERE u.nameid = $1 AND u.job = $2 ;

        PREPARE hatrac_version_list(int8, int8) AS
          SELECT v.*, n.name, n.pid, n.ancestors
          FROM hatrac.name n
          JOIN hatrac.version v ON (v.nameid = n.id)
          WHERE v.nameid = $1 AND NOT v.is_deleted 
          ORDER BY v.id DESC
          LIMIT $2 ;

        PREPARE hatrac_chunk_list (int8, int8) AS
          SELECT *
          FROM hatrac.chunk
          WHERE uploadid = $1 AND ($2 IS NULL OR position = $2)
          ORDER BY position ;

        PREPARE hatrac_object_enumerate_versions (int8) AS
          SELECT n.name, n.pid, n.ancestors, n.subtype, n.update, n."subtree-owner", n."subtree-read", v.*, %(owner_acl)s, %(read_acl)s
          FROM hatrac.name n
          JOIN hatrac.version v ON (v.nameid = n.id)
          WHERE v.nameid = $1 AND NOT v.is_deleted ;

        PREPARE hatrac_namepattern_enumerate_versions (text) AS
          SELECT n.name, n.pid, n.ancestors, n.subtype, n.update, n."subtree-owner", n."subtree-read", v.*, %(owner_acl)s, %(read_acl)s
          FROM hatrac.name n
          JOIN hatrac.version v ON (v.nameid = n.id)
          WHERE n.name ~ $1 AND NOT v.is_deleted ;

        PREPARE hatrac_namespace_children_noacl (int8) AS
          SELECT n.*
          FROM hatrac.name p
          JOIN hatrac.name n ON (n.pid = p.id)
          WHERE p.id = $1 AND NOT n.is_deleted ;

        PREPARE hatrac_namespace_children_acl (int8) AS
          SELECT n.*, %(owner_acl)s, %(update_acl)s, %(read_acl)s, %(create_acl)s
          FROM hatrac.name p
          JOIN hatrac.name n ON (n.pid = p.id)
          WHERE p.id = $1 AND NOT n.is_deleted ;

        PREPARE hatrac_namespace_subtree_noacl (int8) AS
          SELECT n.*
          FROM hatrac.name p
          JOIN hatrac.name n ON (p.id = ANY( n.ancestors ))
          WHERE p.id = $1 AND NOT n.is_deleted ;

        PREPARE hatrac_namespace_subtree_acl (int8) AS
          SELECT n.*, %(owner_acl)s, %(update_acl)s, %(read_acl)s, %(create_acl)s
          FROM hatrac.name p
          JOIN hatrac.name n ON (p.id = ANY( n.ancestors ))
          WHERE p.id = $1 AND NOT n.is_deleted ;

        PREPARE hatrac_object_uploads (int8) AS 
          SELECT u.*, n.name, n.pid, n.ancestors, %(owner_acl)s
          FROM hatrac.name n
          JOIN hatrac.upload u ON (u.nameid = n.id)
          WHERE n.id = $1 ;

        PREPARE hatrac_namespace_uploads (int8) AS
          SELECT u.*, n.name, n.pid, n.ancestors, %(owner_acl)s
          FROM hatrac.name n
          JOIN hatrac.upload u ON (u.nameid = n.id)
          WHERE $1 = ANY (n.ancestors);

""" % dict(
    owner_acl=ancestor_acl_sql('owner'),
    update_acl=ancestor_acl_sql('update'),
    read_acl=ancestor_acl_sql('read'),
    create_acl=ancestor_acl_sql('create')
)
        )
        
        cur.close()
        self.commit()

def pool(minconn, maxconn, dsn):
    """Open a thread-safe connection pool with minconn <= N <= maxconn connections to database.

       The connections are from the customized connection factory in this module.
    """
    return psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, dsn=dsn, connection_factory=connection, cursor_factory=DictCursor)

class PoolManager (object):
    """Manage a set of database connection pools keyed by database name.

    """
    def __init__(self):
        # map dsn -> [pool, timestamp]
        self.pools = dict()
        self.max_idle_seconds = 60 * 60 # 1 hour

    def __getitem__(self, dsn):
        """Lookup existing or create new pool for database on demand.

           May fail transiently and caller should retry.

        """
        # abandon old pools so they can be garbage collected
        for key in list(self.pools.keys()):
            try:
                pair = self.pools.pop(key)
                delta = (datetime.datetime.now() - pair[1])
                try:
                    delta_seconds = delta.total_seconds()
                except:
                    delta_seconds = delta.seconds + delta.microseconds * math.pow(10,-6)
                    
                if delta_seconds < self.max_idle_seconds:
                    # this pool is sufficiently active so put it back!
                    boundpair = self.pools.setdefault(key, pair)
                # if pair is still removed at this point, let garbage collector deal with it
            except KeyError:
                # another thread could have purged key before we got to it
                pass

        try:
            pair = self.pools[dsn]
            pair[1] = datetime.datetime.now() # update timestamp
            return pair[0]
        except KeyError:
            # atomically get/set pool
            newpool = pool(1, 4, dsn)
            boundpair = self.pools.setdefault(dsn, [newpool, datetime.datetime.now()])
            if boundpair[0] is not newpool:
                # someone beat us to it
                newpool.closeall()
            return boundpair[0]
            
pools = PoolManager()       

class PooledConnection (object):
    def __init__(self, dsn):
        self.dsn = dsn

    def perform(self, bodyfunc, finalfunc=lambda x: x, verbose=False):
        """Run bodyfunc(conn, cur) using pooling, commit, transform with finalfunc, clean up.
        
           Automates handling of errors.
        """
        used_pool = pools[self.dsn]
        conn = used_pool.getconn()
        assert conn is not None
        assert conn.status == psycopg2.extensions.STATUS_READY, ("pooled connection status", conn.status)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        cur = conn.cursor(cursor_factory=DictCursor)

        try:
            try:
                result = bodyfunc(conn, cur)
                conn.commit()
                return finalfunc(result)
            except psycopg2.InterfaceError as e:
                # reset bad connection
                used_pool.putconn(conn, close=True)
                conn = None
                raise e
            except GeneratorExit as e:
                # happens normally at end of result yielding sequence
                raise
            except:
                if conn is not None:
                    conn.rollback()
                if verbose:
                    et, ev, tb = sys.exc_info()
                    web.debug(u'got exception "%s" during PooledConnection.perform()' % (ev,),
                              traceback.format_exception(et, ev, tb))
                raise
        finally:
            if conn is not None:
                assert conn.status == psycopg2.extensions.STATUS_READY, ("pooled connection status", conn.status)
                cur.close()
                used_pool.putconn(conn)
                conn = None

_name_table_sql = """
CREATE TABLE IF NOT EXISTS hatrac.name (
  id bigserial PRIMARY KEY,
  pid int8 REFERENCES hatrac."name" (id),
  ancestors int8[],
  name text NOT NULL UNIQUE,
  subtype int NOT NULL,
  is_deleted bool NOT NULL,
  owner text[],
  "create" text[],
  "update" text[],
  read text[],
  "subtree-owner" text[],
  "subtree-create" text[],
  "subtree-update" text[],
  "subtree-read" text[]
);

CREATE INDEX IF NOT EXISTS name_ancestors_idx ON hatrac."name" USING gin (ancestors) WHERE NOT is_deleted;
CREATE INDEX IF NOT EXISTS name_id_idx ON hatrac."name" (id) WHERE "subtree-owner" IS NOT NULL;
CREATE INDEX IF NOT EXISTS name_id_idx1 ON hatrac."name" (id) WHERE "subtree-create" IS NOT NULL;
CREATE INDEX IF NOT EXISTS name_id_idx2 ON hatrac."name" (id) WHERE "subtree-read" IS NOT NULL;
CREATE INDEX IF NOT EXISTS name_id_idx3 ON hatrac."name" (id) WHERE "subtree-update" IS NOT NULL;

INSERT INTO hatrac.name 
(name, ancestors, subtype, is_deleted)
VALUES ('/', array[]::int8[], 0, False)
ON CONFLICT (name) DO NOTHING ;
"""

_version_table_sql = """
CREATE TABLE IF NOT EXISTS hatrac.version (
  id bigserial PRIMARY KEY,
  nameid int8 NOT NULL REFERENCES hatrac.name(id),
  version text,
  nbytes int8,
  metadata jsonb,
  is_deleted bool NOT NULL,
  owner text[],
  read text[],
  UNIQUE(nameid, version),
  CHECK(version IS NOT NULL OR is_deleted)
);

CREATE INDEX IF NOT EXISTS version_nameid_id_idx ON hatrac.version (nameid, id);
"""

_upload_table_sql = """
CREATE TABLE IF NOT EXISTS hatrac.upload (
  id bigserial PRIMARY KEY,
  nameid int8 NOT NULL REFERENCES hatrac.name(id),
  job text NOT NULL,
  nbytes int8 NOT NULL,
  chunksize int8 NOT NULL,
  metadata jsonb,
  owner text[],
  UNIQUE(nameid, job),
  CHECK(chunksize > 0)
);
"""

_chunk_table_sql = """
CREATE TABLE IF NOT EXISTS hatrac.chunk (
  uploadid int8 NOT NULL REFERENCES hatrac.upload(id),
  position int8 NOT NULL,
  aux json,
  UNIQUE(uploadid, position)
);
"""

def db_wrap(reload_pos=None, transform=lambda x: x, enforce_acl=None):
    """Decorate a HatracDirectory method whose body should run in pc.perform(...)

       If reload_pos is not None: 
          replace args[reload_pos] with args[reload_pos]._reload(conn, cur)

       If enforce_acl is (rpos, cpos, acls):
          call args[rpos].enforce_acl(acls, args[cpos])

       Transform result as transform(result).
    """
    def helper(original_method):
        def wrapper(*args, **kwargs):
            def db_thunk(conn, cur):
                args1 = list(args)
                kwargs1 = dict(kwargs)
                if reload_pos is not None:
                    args1[reload_pos] = args1[reload_pos]._reload(conn, cur)
                if enforce_acl is not None:
                    rpos, cpos, acls = enforce_acl
                    args1[rpos].enforce_acl(acls, args[cpos])
                kwargs1['conn'] = conn
                kwargs1['cur'] = cur
                return original_method(*args1, **kwargs1)
            conn = kwargs.get('conn')
            cur = kwargs.get('cur')
            if conn is not None and cur is not None:
                # allow nested calls to db-wrapped functions to run in same outer transaction
                return transform(db_thunk(conn, cur))
            else:
                return args[0].pc.perform(db_thunk, transform)
        return wrapper
    return helper

def ancestor_acl_sql(access):
    """Generate SQL subqueries to compute ancestor ACL arrays for listed accesses.
  
       The SQL fragments are scalar subqueries with an alias:

         (SELECT array_agg(...) ... WHERE n.name = ...) AS "ancestor_access"  

       suitable for inclusion in a SQL SELECT clause.  This SQL
       expects the table alias "n" to be bound to the "name" table
       instance for which ancestor ACLs are being constructed.

    """
    return '''
(SELECT array_agg(DISTINCT s.r)
 FROM (SELECT unnest(%(aclcol)s) AS r
       FROM hatrac.name a
       WHERE a.id = ANY ( n.ancestors )
         AND a.%(aclcol)s IS NOT NULL
 ) s
) AS %(acl)s''' % dict(
    aclcol=sql_identifier('subtree-%s' % access),
    acl=sql_identifier('ancestor_%s' % access)
)

class HatracDirectory (object):
    """Stateful Hatrac Directory tracks bound names and object versions.

    """
    def __init__(self, config, storage):
        self.storage = storage
        self.prefix = config.get('service_prefix')
        self.pc = PooledConnection(config.database_dsn)

    @staticmethod
    def metadata_from_http(metadata):
        return Metadata.from_http(metadata)
        
    @db_wrap()
    def schema_upgrade(self, conn=None, cur=None):
        cur.execute("""
SELECT bool_or(True) AS has_ancestors FROM information_schema.columns
WHERE table_schema = 'hatrac' AND table_name = 'name' AND column_name = 'ancestors' ;
"""
        )
        if not cur.fetchone()[0]:
            cur.execute("""
ALTER TABLE hatrac."name" ADD COLUMN ancestors int8[];
UPDATE hatrac."name" SET ancestors = ARRAY[]::int8[];
CREATE INDEX ON hatrac."name" (id) WHERE "subtree-owner" IS NOT NULL;
CREATE INDEX ON hatrac."name" (id) WHERE "subtree-create" IS NOT NULL;
CREATE INDEX ON hatrac."name" (id) WHERE "subtree-read" IS NOT NULL;
CREATE INDEX ON hatrac."name" (id) WHERE "subtree-update" IS NOT NULL;
""")
            cur.execute("""
SELECT max( array_length(regexp_split_to_array(substring(n.name from 2), '/'), 1) ) maxdepth
FROM name n
WHERE id > 1;
""")[0].maxdepth
            maxdepth = cur.fetchone()[0]
            
            for i in range(maxdepth):
                cur.execute("""
UPDATE name n SET ancestors = a.ancestors || a.id
FROM (
  SELECT n2.id, regexp_split_to_array(substring(n2.name from 2), '/') as name_parts
  FROM name n2
) n2,
name a
WHERE n.id = n2.id
  AND a.name = ('/' || array_to_string(n2.name_parts[1:array_length(n2.name_parts, 1)-1], '/'))
  AND a.ancestors IS NOT NULL
  AND n.id > 1
""")
            sys.stderr.write('added ancestors column to name table\n')

        cur.execute("""
SELECT bool_or(True) AS has_pid FROM information_schema.columns
WHERE table_schema = 'hatrac' AND table_name = 'name' AND column_name = 'pid';
"""
        )
        if not cur.fetchone()[0]:
            cur.execute("""
ALTER TABLE hatrac."name" ADD COLUMN "pid" int8 REFERENCES "name" (id);
UPDATE hatrac."name" c SET pid = p.id
FROM (
  SELECT n2.id, regexp_split_to_array(substring(n2.name from 2), '/') as name_parts
  FROM name n2
) c2,
name p
WHERE c.id = c2.id
  AND p.name = ('/' || array_to_string(c2.name_parts[1:array_length(c2.name_parts, 1)-1], '/'))
;
""")
            sys.stderr.write('added pid column to name table\n')

        cur.execute("""
SELECT table_name
FROM information_schema.columns
WHERE table_schema = 'hatrac' 
  AND table_name = ANY (ARRAY['version', 'upload'])
  AND column_name = 'content_type'
EXCEPT
SELECT table_name
FROM information_schema.columns
WHERE table_schema = 'hatrac' 
  AND table_name = ANY (ARRAY['version', 'upload'])
  AND column_name = 'metadata';
""")
        for tname in [ row[0] for row in cur ]:
            sys.stderr.write('converting %s to have metadata column... ' % tname)
            cur.execute("""
ALTER TABLE hatrac.%(table)s ADD COLUMN metadata jsonb;
UPDATE hatrac.%(table)s SET metadata = (
  CASE WHEN content_type IS NOT NULL THEN jsonb_build_object('content-type', content_type) ELSE '{}'::jsonb END
  || CASE WHEN content_md5 IS NOT NULL THEN jsonb_build_object('content-md5', content_md5) ELSE '{}'::jsonb END
);
ALTER TABLE hatrac.%(table)s DROP COLUMN content_type;
ALTER TABLE hatrac.%(table)s DROP COLUMN content_md5;
ALTER TABLE hatrac.%(table)s ALTER COLUMN metadata SET NOT NULL;
""" % dict(table=sql_identifier(tname))
            )
            sys.stderr.write('done.\n')
        
    @db_wrap()
    def deploy_db(self, admin_roles, conn=None, cur=None):
        """Initialize database and set root namespace owners."""
        cur.execute('CREATE SCHEMA IF NOT EXISTS hatrac')
        for sql in [
                _name_table_sql,
                _version_table_sql,
                _upload_table_sql,
                _chunk_table_sql
        ]:
            cur.execute(sql)

        # prepare statements again since they would have failed prior to above deploy SQL steps...
        conn._prepare_hatrac_stmts()

        rootns = HatracNamespace(self, **(self._name_lookup(conn, cur, '/')))

        for role in admin_roles:
            self._set_resource_acl_role(conn, cur, rootns, 'owner', role)
            
    @db_wrap()
    def create_name(self, name, is_object, make_parents, client_context, conn=None, cur=None):
        """Create, persist, and return a HatracNamespace or HatracObject instance.

        """
        nameparts = [ n for n in name.split('/') if n ]
        parname = "/" + "/".join(nameparts[0:-1])
        relname = nameparts[-1]
        if relname in [ '.', '..' ]:
            raise core.BadRequest('Illegal name "%s".' % relname)

        try:
            resource = HatracName.construct(self, **self._name_lookup(conn, cur, name, False))
            if resource.is_deleted:
                raise core.Conflict('Name %s not available.' % resource)
            else:
                raise core.Conflict('Name %s already in use.' % resource)
        except core.NotFound as ev:
            pass

        try:
            parent = HatracName.construct(self, **self._name_lookup(conn, cur, parname))
            if parent.is_object():
                raise core.Conflict('Parent %s is not a namespace.' % (self.prefix + parname))
        except core.NotFound:
            if make_parents:
                parent = self.create_name(parname, False, True, client_context, conn=conn, cur=cur)
            else:
                raise

        parent.enforce_acl(['owner', 'create', 'ancestor_owner', 'ancestor_create'], client_context)
        resource = HatracName.construct(self, **self._create_name(conn, cur, name, parent.id, parent.ancestors + [parent.id], is_object))
        self._set_resource_acl_role(conn, cur, resource, 'owner', client_context.client)
        return resource

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner', 'ancestor_owner']), transform=lambda thunk: thunk())
    def delete_name(self, resource, client_context, conn=None, cur=None):
        """Delete an existing namespace or object resource."""
        if resource.name == '/':
            raise core.Forbidden('Root service namespace %s cannot be deleted.' % resource)

        # test ACLs and map out recursive delete
        deleted_uploads = []
        deleted_versions = []
        deleted_names = []

        for row in self._namespace_enumerate_uploads(conn, cur, resource):
            deleted_uploads.append( web.storage(row) )

        for row in self._namespace_enumerate_versions(conn, cur, resource):
            obj = HatracObject(self, **row)
            HatracObjectVersion(self, obj, **row).enforce_acl(['owner', 'subtree-owner', 'ancestor_owner'], client_context)
            deleted_versions.append( web.storage(row) )

        for row in self._namespace_enumerate_names(conn, cur, resource):
            HatracName.construct(self, **row).enforce_acl(['owner', 'ancestor_owner'], client_context)
            deleted_names.append( web.storage(row) )

        # we only get here if no ACL raised an exception above
        deleted_names.append(resource)

        for res in deleted_uploads:
            self._delete_upload(conn, cur, res)
        for res in deleted_versions:
            self._delete_version(conn, cur, res)
        for res in deleted_names:
            self._delete_name(conn, cur, res)

        def cleanup():
            # tell storage system to clean up after deletes were committed to DB
            for res in deleted_uploads:
                self.storage.cancel_upload(res.name, res.job)
            for res in deleted_versions:
                self.storage.delete(res.name, res.version)
            for res in deleted_names:
                self.storage.delete_namespace(res.name)

        return cleanup

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner', 'ancestor_owner']), transform=lambda thunk: thunk())
    def delete_version(self, resource, client_context, conn=None, cur=None):
        """Delete an existing version."""
        self._delete_version(conn, cur, resource)
        return lambda : self.storage.delete(resource.name, resource.version)

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner', 'update', 'ancestor_owner', 'ancestor_update']))
    def create_version(self, object, client_context, nbytes=None, metadata={}, conn=None, cur=None):
        """Create, persist, and return a HatracObjectVersion instance.

           Newly created instance is marked 'deleted'.
        """
        v = self._create_version(conn, cur, object, nbytes, metadata)
        resource = HatracObjectVersion(self, object, **v)
        self._set_resource_acl_role(conn, cur, resource, 'owner', client_context.client)
        return resource

    def create_version_from_file(self, object, input, client_context, nbytes, metadata={}):
        """Create, persist, and return HatracObjectVersion with given content.

        """
        resource = self.create_version(object, client_context, nbytes, metadata)
        version = self.storage.create_from_file(object.name, input, nbytes, metadata)
        self.pc.perform(lambda conn, cur: self._complete_version(conn, cur, resource, version))
        return self.version_resolve(object, version)

    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner', 'update', 'ancestor_owner', 'ancestor_update']))
    def create_version_upload_job(self, object, chunksize, client_context, nbytes=None, metadata={}, conn=None, cur=None):
        job = self.storage.create_upload(object.name, nbytes, metadata)
        resource = HatracUpload(self, object, **self._create_upload(conn, cur, object, job, chunksize, nbytes, metadata))
        self._set_resource_acl_role(conn, cur, resource, 'owner', client_context.client)
        return resource

    def upload_chunk_from_file(self, upload, position, input, client_context, nbytes, metadata={}):
        upload.enforce_acl(['owner'], client_context)
        nchunks = upload.nbytes / upload.chunksize
        remainder = upload.nbytes % upload.chunksize
        assert position >= 0
        if position < (nchunks - 1) and nbytes != upload.chunksize:
            raise core.Conflict('Uploaded chunk byte count %s does not match job chunk size %s.' % (nbytes, upload.chunksize))
        if remainder and position == nchunks and nbytes != remainder:
            raise core.Conflict('Uploaded chunk byte count %s does not match final chunk size %s.' % (nbytes, remainder))
        if position > nchunks or position == nchunks and remainder == 0:
            raise core.Conflict('Uploaded chunk number %s out of range.' % position)
        aux = self.storage.upload_chunk_from_file(
            upload.object.name, 
            upload.job, 
            position, 
            upload.chunksize, 
            input, 
            nbytes, 
            metadata
        )

        def db_thunk(conn, cur):
            self._track_chunk(conn, cur, upload, position, aux)

        if self.storage.track_chunks:
            self.pc.perform(db_thunk)

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner']))
    def upload_finalize(self, upload, client_context, conn=None, cur=None):
        if self.storage.track_chunks:
            chunk_aux = list(self._chunk_list(conn, cur, upload))
        else:
            chunk_aux = None
        version_id = self.storage.finalize_upload(upload.name, upload.job, chunk_aux, metadata=upload.metadata)
        version = HatracObjectVersion(self, upload.object, **self._create_version(conn, cur, upload.object, upload.nbytes, upload.metadata))
        self._set_resource_acl_role(conn, cur, version, 'owner', client_context.client)
        self._complete_version(conn, cur, version, version_id)
        self._delete_upload(conn, cur, upload)
        version.version = version_id
        return version

    @db_wrap(reload_pos=1, enforce_acl=(1, 2, ['owner', 'ancestor_owner']), transform=lambda thunk: thunk())
    def upload_cancel(self, upload, client_context, conn=None, cur=None):
        self._delete_upload(conn, cur, upload)
        return lambda : self.storage.cancel_upload(upload.name, upload.job)

    @db_wrap(reload_pos=2, enforce_acl=(2, 4, ['owner', 'read', 'ancestor_owner', 'ancestor_read']))
    def get_version_content_range(self, object, objversion, get_slice, client_context, get_data=True, conn=None, cur=None):
        """Return (nbytes, data_generator) pair for specific version."""
        if objversion.is_deleted:
            raise core.NotFound('Resource %s is not available.' % objversion)
        if get_data:
            nbytes, metadata, data = self.storage.get_content_range(object.name, objversion.version, objversion.metadata, get_slice)
        else:
            nbytes = objversion.nbytes
            metadata = objversion.metadata
            data = ''
        return nbytes, metadata, data

    def get_version_content(self, object, objversion, client_context, get_data=True):
        """Return (nbytes, data_generator) pair for specific version."""
        return self.get_version_content_range(object, objversion, None, client_context, get_data)

    @db_wrap()
    def name_resolve(self, name, raise_notfound=True, conn=None, cur=None):
        """Return a HatracNamespace or HatracObject instance.
        """
        try:
            return HatracName.construct(self, **self._name_lookup(conn, cur, name))
        except core.NotFound as ev:
            if raise_notfound:
                raise ev

    @db_wrap()
    def version_resolve(self, object, version, raise_notfound=True, conn=None, cur=None):
        """Return a HatracObjectVersion instance corresponding to referenced version.
        """
        return HatracObjectVersion(
            self, 
            object, 
            **self._version_lookup(conn, cur, object, version, not raise_notfound)
        )

    @db_wrap(reload_pos=1)
    def upload_resolve(self, object, job, raise_notfound=True, conn=None, cur=None):
        """Return a HatracUpload instance corresponding to referenced job.
        """
        return HatracUpload(self, object, **self._upload_lookup(conn, cur, object, job))

    @db_wrap(reload_pos=1)
    def get_current_version(self, object, conn=None, cur=None):
        """Return a HatracObjectVersion instance corresponding to latest.
        """
        assert object.id is not None, object
        results = self._version_list(conn, cur, object.id, limit=1)
        if results:
            return HatracObjectVersion(self, object, **results[0])
        else:
            raise core.Conflict('Object %s currently has no content.' % object)

    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner', 'ancestor_owner']))
    def update_resource_metadata(self, resource, updates, client_context, conn=None, cur=None):
        self._update_resource_metadata(conn, cur, resource, updates)
        
    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner', 'ancestor_owner']))
    def pop_resource_metadata(self, resource, fieldname, client_context, conn=None, cur=None):
        self._pop_resource_metadata(conn, cur, resource, fieldname)
        
    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner', 'ancestor_owner']))
    def set_resource_acl_role(self, resource, access, role, client_context, conn=None, cur=None):
        self._set_resource_acl_role(conn, cur, resource, access, role)

    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner', 'ancestor_owner']))
    def drop_resource_acl_role(self, resource, access, role, client_context, conn=None, cur=None):
        if role not in resource.acls[access]:
            raise core.NotFound('Resource %s;acl/%s/%s not found.' % (resource, access, role))
        self._drop_resource_acl_role(conn, cur, resource, access, role)

    @db_wrap(reload_pos=1, enforce_acl=(1, 4, ['owner', 'ancestor_owner']))
    def set_resource_acl(self, resource, access, acl, client_context, conn=None, cur=None):
        self._set_resource_acl(conn, cur, resource, access, acl)

    @db_wrap(reload_pos=1, enforce_acl=(1, 3, ['owner', 'ancestor_owner']))
    def clear_resource_acl(self, resource, access, client_context, conn=None, cur=None):
        self._set_resource_acl(conn, cur, resource, access, [])

    @db_wrap(reload_pos=1)
    def object_enumerate_versions(self, object, conn=None, cur=None):
        """Return a list of versions
        """
        return [
            HatracObjectVersion(self, HatracName.construct(self, subtype=1, **row), **row)
            for row in self._version_list(conn, cur, object.id) 
        ]

    @db_wrap(reload_pos=1)
    def namespace_enumerate_names(self, resource, recursive=True, need_acls=True, conn=None, cur=None):
        return [
            HatracName.construct(self, **row)
            for row in self._namespace_enumerate_names(conn, cur, resource, recursive, need_acls)
        ]

    @db_wrap(reload_pos=1)
    def namespace_enumerate_uploads(self, resource, recursive=True, conn=None, cur=None):
        return [
            HatracUpload(self, HatracName.construct(self, subtype=1, **row), **row)
            for row in self._namespace_enumerate_uploads(conn, cur, resource, recursive) 
        ]

    def _update_resource_metadata(self, conn, cur, resource, updates):
        resource.metadata.update(updates)
        cur.execute("""
UPDATE hatrac.%(table)s n
SET metadata = %(metadata)s
WHERE n.id = %(id)s ;
""" % dict(
    table=sql_identifier(resource._table_name),
    id=sql_literal(resource.id),
    metadata=sql_literal(resource.metadata.to_sql())
)
        )

    def _pop_resource_metadata(self, conn, cur, resource, fieldname):
        resource.metadata.pop(fieldname)
        cur.execute("""
UPDATE hatrac.%(table)s n
SET metadata = %(metadata)s
WHERE n.id = %(id)s ;
""" % dict(
    table=sql_identifier(resource._table_name),
    id=sql_literal(resource.id),
    metadata=sql_literal(resource.metadata.to_sql())
)
        )
        
    def _set_resource_acl_role(self, conn, cur, resource, access, role):
        if access not in resource._acl_names:
            raise core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        role = role['id'] if type(role) is dict else role
        # need to use raw SQL to compute modified array in database
        cur.execute("""
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

    def _drop_resource_acl_role(self, conn, cur, resource, access, role):
        if access not in resource._acl_names:
            raise core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        role = role['id'] if type(role) is dict else role
        # need to use raw SQL to compute modified array in database
        cur.execute("""
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

    def _set_resource_acl(self, conn, cur, resource, access, acl):
        if access not in resource._acl_names:
            raise core.BadRequest('Invalid ACL name %s for %s.' % (access, resource))
        cur.execute("""
UPDATE hatrac.%(table)s r 
SET %(acl)s = ARRAY[%(roles)s]::text[]
WHERE r.id = %(id)s 
""" % dict(
    table=sql_identifier(resource._table_name),
    id=sql_literal(resource.id),
    acl=sql_identifier(access),
    roles=','.join(map(sql_literal, acl))
)
        )
        resource.acls[access] = ACL(acl)

    def _create_name(self, conn, cur, name, pid, ancestors, is_object=False):
        cur.execute("""
INSERT INTO hatrac.name
(name, pid, ancestors, subtype, is_deleted)
VALUES (%(name)s, %(pid)s, ARRAY[%(ancestors)s]::int8[], %(isobject)s, False)
RETURNING *
""" % dict(
    name=sql_literal(name),
    pid=sql_literal(pid),
    ancestors=','.join([ sql_literal(a) for a in ancestors ]),
    isobject=is_object and 1 or 0
)
        )
        return list(cur)[0]

    def _create_version(self, conn, cur, object, nbytes=None, metadata={}):
        cur.execute("""
INSERT INTO hatrac.version
(nameid, nbytes, metadata, is_deleted)
VALUES (%(nameid)s, %(nbytes)s, %(metadata)s, True)
RETURNING *, %(name)s AS "name", %(pid)s AS pid, ARRAY[%(ancestors)s]::int8[] AS "ancestors"
""" % dict(
    name=sql_literal(object.name),
    nameid=sql_literal(object.id),
    pid=sql_literal(object.pid),
    ancestors=','.join([sql_literal(a) for a in object.ancestors]),
    nbytes=nbytes is not None and sql_literal(int(nbytes)) or 'NULL::int8',
    metadata=sql_literal(metadata.to_sql())
)
        )
        return list(cur)[0]

    def _create_upload(self, conn, cur, object, job, chunksize, nbytes, metadata):
        cur.execute("""
INSERT INTO hatrac.upload 
(nameid, job, nbytes, chunksize, metadata)
VALUES (%(nameid)s, %(job)s, %(nbytes)s, %(chunksize)s, %(metadata)s)
RETURNING *, %(name)s AS "name", %(pid)s AS pid, ARRAY[%(ancestors)s]::int8[] AS "ancestors"
""" % dict(
    name=sql_literal(object.name),
    ancestors=','.join([sql_literal(a) for a in object.ancestors]),
    nameid=sql_literal(object.id),
    pid=sql_literal(object.pid),
    job=sql_literal(job),
    nbytes=sql_literal(int(nbytes)),
    chunksize=sql_literal(int(chunksize)),
    metadata=sql_literal(metadata.to_sql())
)
        )
        return list(cur)[0]

    def _track_chunk(self, conn, cur, upload, position, aux):
        sql_fields = dict(
            uploadid=sql_literal(upload.id),
            position=sql_literal(int(position)),
            aux=sql_literal(jsonWriter(aux))
        )
        
        try:
            result = self._chunk_lookup(conn, cur, upload, position)
            cur.execute("""
UPDATE hatrac.chunk
SET aux = %(aux)s
WHERE uploadid = %(uploadid)s AND position = %(position)s
""" % sql_fields
            )
            
        except core.NotFound:
            cur.execute("""
INSERT INTO hatrac.chunk
(uploadid, position, aux)
VALUES (%(uploadid)s, %(position)s, %(aux)s)
""" % sql_fields
            )

    def _complete_version(self, conn, cur, resource, version, is_deleted=False):
        cur.execute("EXECUTE hatrac_complete_version(%s, %s, %s);" % (
            sql_literal(resource.id),
            sql_literal(version),
            sql_literal(is_deleted)
        ))

    def _delete_name(self, conn, cur, resource):
        cur.execute("EXECUTE hatrac_delete_name(%s);" % sql_literal(resource.id))

    def _delete_version(self, conn, cur, resource):
        cur.execute("EXECUTE hatrac_delete_version(%s);" % sql_literal(resource.id))

    def _delete_upload(self, conn, cur, resource):
        cur.execute("""
EXECUTE hatrac_delete_chunks(%(id)s);
EXECUTE hatrac_delete_upload(%(id)s);
""" % dict(id=sql_literal(resource.id))
        )

    def _name_lookup(self, conn, cur, name, check_deleted=True):
        cur.execute("EXECUTE hatrac_name_lookup(%s, %s);" % (
            sql_literal(name),
            sql_literal(check_deleted)
        ))
        for row in list(cur):
            return row
        raise core.NotFound('Resource %s not found.' % (self.prefix + name))
        
    def _version_lookup(self, conn, cur, object, version, allow_deleted=True):
        cur.execute("EXECUTE hatrac_version_lookup(%s, %s);" % (
            sql_literal(int(object.id)),
            sql_literal(version)
        ))
        for row in list(cur):
            if row['is_deleted'] and not allow_deleted:
                raise core.NotFound("Resource %s:%s not available." % (object, version))
            else:
                row['metadata'] = Metadata.from_sql(row['metadata'])
                return row
        raise core.NotFound("Resource %s:%s not found." % (object, version))

    def _upload_lookup(self, conn, cur, object, job):
        cur.execute("EXECUTE hatrac_upload_lookup(%s, %s);" % (
            sql_literal(int(object.id)),
            sql_literal(job)
        ))
        for row in list(cur):
            row['metadata'] = Metadata.from_sql(row['metadata'])
            return row
        raise core.NotFound("Resource %s;upload/%s not found." % (object, job))
        
    def _chunk_lookup(self, conn, cur, upload, position):
        result = self._chunk_list(conn, cur, upload, position)
        if not result:
            raise core.NotFound("Resource %s/%s not found." % (upload, position))
        return result[0]
        
    def _version_list(self, conn, cur, nameid, limit=None):
        # TODO: add range keying for scrolling enumeration?
        cur.execute("EXECUTE hatrac_version_list(%d, %s);" % (
            nameid,
            ("%d" % limit) if limit is not None else 'NULL'
        ))
        def helper(row):
            row['metadata'] = Metadata.from_sql(row['metadata'])
            return row
        return [ helper(row) for row in cur ]
        
    def _chunk_list(self, conn, cur, upload, position=None):
        cur.execute("EXECUTE hatrac_chunk_list(%s, %s);" % (
            sql_literal(int(upload.id)),
            sql_literal(int(position)) if position is not None else 'NULL'
        ))
        result = list(cur)
        if not result:
            raise core.NotFound("Chunk data %s/%s not found." % (upload, position))
        return result

    def _namespace_enumerate_versions(self, conn, cur, resource):
        # return every version under /name... or /name/
        if resource.is_object():
            cur.execute("EXECUTE hatrac_object_enumerate_versions(%s);" % sql_literal(int(resource.id)))
        else:
            cur.execute("EXECUTE hatrac_namepattern_enumerate_versions(%s);" % sql_literal(sql_literal("^" + regexp_escape(resource.name) + '/')))
        def helper(row):
            row['metadata'] = Metadata.from_sql(row['metadata'])
            return row
        return [ helper(row) for row in cur ]

    def _namespace_enumerate_names(self, conn, cur, resource, recursive=True, need_acls=True):
        cur.execute("EXECUTE hatrac_namespace_%s_%sacl (%s);" % (
            'subtree' if recursive else 'children',
            '' if need_acls else 'no',
            sql_literal(int(resource.id))
        ))
        return list(cur)
    
    def _namespace_enumerate_uploads(self, conn, cur, resource, recursive=True):
        # return every upload under /name... or /name/
        cur.execute("EXECUTE hatrac_%s_uploads (%s);" % (
            'object' if resource.is_object() else 'namespace',
            sql_literal(int(resource.id))
        ))
        return list(cur)
