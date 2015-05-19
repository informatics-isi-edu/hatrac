
The current status is that basic APIs seem to work: 
 
1. namespace GET, PUT, DELETE, HEAD
1. object GET, PUT, DELETE, HEAD
1. object version GET, DELETE, HEAD
1. list object versions with GET, HEAD on versions sub-resource
1. ACLs GET, HEAD
1. ACL GET, PUT, DELETE, HEAD
1. ACL entry GET, PUT, DELETE, HEAD
1. ACL-based authorization of all requests
1. integrity checking when Content-MD5 is present
1. chunked upload via POST, PUT..., POST sequence
1. chunked upload status via GET, HEAD
1. chunked upload job listing via GET, HEAD
1. chunked upload cancel via DELETE
1. partial download via GET with Range header
1. storage using filesystem or S3 versioned bucket
1. ETag, If-Match, If-None-Match concurrency control
  - useful for object, object version, ACL management
  - supported on all methods
  - allows GET caching and precondition protection on PUT/DELETE
1. custom error messages for 401 or 403 responses supported with trivial templating
  - set 401_html and/or 403_html config values in hatrac_config.json
  - use `%(message)s` Python string-interpolation reference to embed service-generated text string into 

This includes the specified immutable/stable reference semantics from 
the REST-API.md doc, i.e. deletion leaves breadcrumbs in the database 
and names cannot be reused for other purposes. 

Known Issues/Limitations:

1. using `Content-Type: application/x-hatrac-namespace` is a pretty
   old-fashioned hack; either register a real MIME type or come up
   with some other solution...?
1. very minimal install/deploy automation

Missing implementation features include: 
 
1. Any support for webauthn2 login w/o external deployment such as ERMrest
 
Missing specification and implementation: 

1. any way to control initial ACLs during PUT operations 
1. any URL-based third-party PUT 
1. any timeout/garbage collection for jobs 
1. any HTTP OPTIONS operations 

Additional TODO considerations:

1. allow PUT to restore a deleted object or namespace using the owner
   ACL on the deleted resource?
1. content-negotiation for namespace or ACL data formats?
