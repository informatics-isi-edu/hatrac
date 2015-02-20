#!/usr/bin/python

import os
from StringIO import StringIO
import hatrac
import hatrac.core
import web
from webauthn2.manager import Context

web.config.debug = False

test_config = web.storage(
    {
    "storage_backend": "filesystem",
    "storage_path": os.getcwd() + "/hatrac_test_data",
    "database_type": "postgres",
    "database_name": os.environ.get("HATRAC_TEST_DB", "hatrac_test"),
    "database_schema": "hatrac",
    "database_max_retries": 5
    }
)

os.mkdir(test_config.storage_path)
test_directory = hatrac.instantiate(test_config)

test_directory.deploy_db(["testroot"])

root_context = Context()
root_context.client = 'testroot'

anon_context = Context()

foo_context = Context()
foo_context.client = 'foo'

def expect(cls, thunk):
    got_expected = False
    try:
        thunk()
    except cls:
        got_expected = True

    assert got_expected, 'expected %s' % cls

rootns = test_directory.name_resolve("/")

expect(
    hatrac.core.NotFound,
    lambda : test_directory.name_resolve("/foo")
)

expect(
    hatrac.core.Conflict, 
    lambda : test_directory.create_name("/foo/bar", False, root_context)
)

test_directory.create_name("/foo", False, root_context)
test_directory.create_name("/foo/bar", False, root_context)
test_directory.create_name("/foo/obj1", True, root_context)
obj1 = test_directory.name_resolve("/foo/obj1")

assert 'testroot' in obj1.get_acls()['owner']

expect(
    hatrac.core.Conflict,
    lambda : obj1.get_current_version()
)

content1 = 'test data 1\n'
nbytes1 = len(content1)

obj1.create_version_from_file(
    StringIO(content1), nbytes1, root_context, 'text/plain'
)

vers1 = obj1.get_current_version()

rbytes1, data1 = obj1.get_content(root_context)
assert rbytes1 == nbytes1
assert ''.join(data1) == content1

content2 = 'test data 2\n'
nbytes2 = len(content2)

obj1.create_version_from_file(
    StringIO(content2), nbytes2, root_context, 'text/plain'
)

vers2 = obj1.get_current_version()

rbytes2, data2 = obj1.get_content(root_context)
assert rbytes2 == nbytes2
assert ''.join(data2) == content2

rbytes1, data1 = vers1.get_content(root_context)
assert rbytes1 == nbytes1
assert ''.join(data1) == content1

rbytes2, data2 = vers2.get_content(root_context)
assert rbytes2 == nbytes2
assert ''.join(data2) == content2

vers2.delete(root_context)

rbytes3, data3 = obj1.get_content(root_context)
assert rbytes3 == nbytes1
assert ''.join(data3) == content1

expect(
    hatrac.core.NotFound,
    lambda : test_directory.name_resolve("%s" % vers2)
)

expect(
    hatrac.core.NotFound,
    lambda : vers2.get_content(root_context)
)

expect(
    hatrac.core.Unauthenticated,
    lambda : obj1.delete(anon_context)
)

expect(
    hatrac.core.Forbidden,
    lambda : obj1.get_content(foo_context)
)

vers1.set_acl('read', ['foo', 'bar'], root_context)

obj1.get_content(foo_context)

vers1.set_acl_role('read', 'baz', root_context)    
vers1.drop_acl_role('read', 'bar', root_context)

expect(
    hatrac.core.NotFound,
    lambda : obj1.drop_acl_role('read', 'bar', root_context)
)

obj1.clear_acl('read', root_context)

obj1.delete(root_context)

