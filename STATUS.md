
The current status is that basic APIs seem to work: 
 
1. namespace GET, PUT, DELETE, HEAD
1. object GET, PUT, DELETE, HEAD
1. object version GET, DELETE, HEAD
1. ACLs GET, HEAD
1. ACL GET, PUT, DELETE, HEAD
1. ACL entry GET, PUT, DELETE, HEAD
1. ACL-based authorization of all requests
1. integrity checking when Content-MD5 is present
1. chunked upload via POST, PUT..., POST sequence
1. chunked upload cancel via DELETE
1. partial download via GET with Range header

This includes the specified immutable/stable reference semantics from 
the REST-API.md doc, i.e. deletion leaves breadcrumbs in the database 
and names cannot be reused for other purposes. 

Known Issues/Limitations:

1. the Hatrac deployment prefix `/hatrac` is missing from URL paths
   generated in responses
1. using `Content-Type: application/x-hatrac-namespace` is a pretty
   old-fashioned hack; either register a real MIME type or come up
   with some other solution...?
1. very minimal install/deploy automation

Missing implementation features include: 
 
1. Any support for webauthn2 login w/o external deployment such as ERMrest
1. Tracking object version content size in database to allow integrity checking
1. S3 storage backend 
 
Missing specification and implementation: 

1. any way to control initial ACLs during PUT operations 
1. any way to list versions of objects 
1. any way to list upload jobs 
1. any URL-based third-party PUT 
1. any timeout/garbage collection for jobs 
1. any HTTP OPTIONS operations 
1. any HTTP cache-control features 

Additional TODO considerations:

1. allow PUT to restore a deleted object or namespace using the owner
   ACL on the deleted resource?
1. content-negotiation for namespace or ACL data formats?
