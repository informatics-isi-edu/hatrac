#!/usr/bin/python

import os
from StringIO import StringIO
import hatrac
import hatrac.core
import web
from webauthn2.manager import Context
import web
import hashlib

web.config.debug = False

if True:
    # do normal testing with filesystem backend
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
else:
    # do testing with S3 backend
    test_config = web.storage(
        {
            "storage_backend": "amazons3",
            "database_type": "postgres",
            "database_name": os.environ.get("HATRAC_TEST_DB", "hatrac_test"),
            "database_schema": "hatrac",
            "database_max_retries": 5,
            "s3_connection": {
                "aws_access_key_id": os.environ["AWS_ACCESS_KEY"],
                "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"]
            }
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
    hatrac.core.Forbidden,
    lambda : rootns.delete(root_context)
)

expect(
    hatrac.core.NotFound,
    lambda : test_directory.name_resolve("/foo")
)

expect(
    hatrac.core.NotFound, 
    lambda : test_directory.create_name("/foo/bar", False, root_context)
)

test_directory.create_name("/foo", False, root_context)
test_directory.create_name("/foo/bar", False, root_context)
test_directory.create_name("/foo/obj1", True, root_context)
test_directory.create_name("/foo/objJ", True, root_context)
obj1 = test_directory.name_resolve("/foo/obj1")
objJ = test_directory.name_resolve("/foo/objJ")

assert 'testroot' in obj1.get_acls()['owner']

expect(
    hatrac.core.Conflict,
    lambda : obj1.get_current_version()
)

content1 = 'test data 1\n'
nbytes1 = len(content1)
content1_md5 = hashlib.md5(content1).hexdigest()

expect(
    hatrac.core.BadRequest,
    lambda : obj1.create_version_from_file(StringIO(content1), root_context, nbytes1, 'text/plain', 'thisisbroken')
)

obj1.create_version_from_file(
    StringIO(content1), root_context, nbytes1, 'text/plain', content1_md5
)

contentJ = 'test data that will be sent in multiple parts'
chunksize = 10
nbytesJ = len(contentJ)
contentJ_md5 = hashlib.md5(contentJ).hexdigest()

upload = objJ.create_version_upload_job(chunksize, root_context, nbytesJ, 'text/plain', contentJ_md5)
pos = 0
while pos < len(contentJ):
    chunk = contentJ[pos:min(pos+chunksize,nbytesJ)]
    chunk_md5 = hashlib.md5(chunk).hexdigest()
    upload.upload_chunk_from_file(pos/chunksize, StringIO(chunk), root_context, len(chunk), chunk_md5)
    pos += chunksize
versJ = upload.finalize(root_context)

''.join(objJ.get_content(root_context)[3])
upload = objJ.create_version_upload_job(chunksize, root_context, nbytesJ, 'text/plain', contentJ_md5)
''.join(objJ.get_content(root_context)[3])
upload.cancel(root_context)
''.join(objJ.get_content(root_context)[3])

vers1 = obj1.get_current_version()

rbytes1, ct1, hash1, data1 = obj1.get_content(root_context)
assert rbytes1 == nbytes1
assert ''.join(data1) == content1

rbytes1, ct1, hash1, data1 = obj1.get_content_range(root_context, slice(2,8))
assert rbytes1 == 6
assert ''.join(data1) == content1[2:8]
assert hash1 is None, hash1

rbytes1, ct1, hash1, data1 = obj1.get_content_range(root_context, slice(2,None))
assert rbytes1 == nbytes1 - 2
assert ''.join(data1) == content1[2:]
assert hash1 is None

rbytes1, ct1, hash1, data1 = obj1.get_content_range(root_context, slice(0,None))
assert rbytes1 == nbytes1
assert ''.join(data1) == content1[0:]
assert hash1 == content1_md5

content2 = 'test data 2\n'
nbytes2 = len(content2)
content2_md5 = hashlib.md5(content2).hexdigest()

obj1.create_version_from_file(
    StringIO(content2), root_context, nbytes2, 'text/plain', content2_md5
)

vers2 = obj1.get_current_version()

rbytes2, ct2, hash2, data2 = obj1.get_content(root_context)
assert rbytes2 == nbytes2
assert ''.join(data2) == content2

rbytes1, ct1, hash1, data1 = vers1.get_content(root_context)
assert rbytes1 == nbytes1
assert ''.join(data1) == content1

rbytes2, ct2, hash2, data2 = vers2.get_content(root_context)
assert rbytes2 == nbytes2
assert ''.join(data2) == content2

# tamper with storage to simulate storage corruption
f = open("%s/%s:%s" % (test_config.storage_path, vers2.name, vers2.version), "ab")
f.write("this is broken")
f.close()
expect(
    IOError,
    lambda: ''.join(vers2.get_content(root_context)[3])
)

vers2.delete(root_context)

rbytes3, ct3, hash3, data3 = obj1.get_content(root_context)
assert rbytes3 == nbytes1
assert ''.join(data3) == content1

expect(
    hatrac.core.NotFound,
    lambda : test_directory.name_resolve("%s" % vers2)
)

expect(
    hatrac.core.NotFound,
    lambda : ''.join(vers2.get_content(root_context)[3])
)

expect(
    hatrac.core.Unauthenticated,
    lambda : obj1.delete(anon_context)
)

expect(
    hatrac.core.Forbidden,
    lambda : ''.join(obj1.get_content(foo_context)[3])
)

vers1.set_acl('read', ['foo', 'bar'], root_context)

''.join(obj1.get_content(foo_context)[3])

vers1.set_acl_role('read', 'baz', root_context)    
vers1.drop_acl_role('read', 'bar', root_context)

expect(
    hatrac.core.NotFound,
    lambda : obj1.drop_acl_role('read', 'bar', root_context)
)

obj1.clear_acl('read', root_context)

obj1.delete(root_context)

