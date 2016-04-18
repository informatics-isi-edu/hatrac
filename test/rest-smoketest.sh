#!/bin/bash

# Run basic Hatrac REST API tests 

usage()
{
    cat <<EOF
usage: $0

Runs test against local host (https://$(hostname)/hatrac/) using
cookie-based authentication.  It is the caller's responsibility to
populate the cookie store with a valid login session.

Uses cookie store named in COOKIES environment ($COOKIES), defaulting
to 'cookie' in the current working directory if not set in
environment.

A successful run will exit with status 0 and an empty standard output.

A failure will exit with status 1 and a non-empty standard output.

Diagnostics may be printed to standard error regardless of success or
failure.  Setting the environment variable VERBOSE=true increases the
amount of diagnostic output.

EOF
}

error()
{
    cat >&2 <<EOF
$0: $*
EOF
    usage >&2
    exit 1
}

[[ -z $GOAUTH  && -z $COOKIES ]] && error
[[ -n $COOKIES && ! -r $COOKIES ]] && error

#Supported deployments: amazons3 or filesystem
[[ -z $DEPLOYMENT ]] && DEPLOYMENT="filesystem"

echo "Using $DEPLOYMENT deployment" >&2

RUNKEY=smoketest-$RANDOM
RESPONSE_HEADERS=/tmp/${RUNKEY}-response-headers
RESPONSE_CONTENT=/tmp/${RUNKEY}-response-content
TEST_DATA=/tmp/${RUNKEY}-test-data

cleanup()
{
    rm -f ${RESPONSE_HEADERS} ${RESPONSE_CONTENT} ${TEST_DATA}
    rm -f /tmp/parts-${RUNKEY}*
    rm -f /tmp/dummy-${RUNKEY}
}

trap cleanup 0

mycurl()
{
    touch ${RESPONSE_HEADERS}
    touch ${RESPONSE_CONTENT}
    truncate -s 0 ${RESPONSE_HEADERS}
    truncate -s 0 ${RESPONSE_CONTENT}
    
    curl_options=(
      -D ${RESPONSE_HEADERS}
      -o ${RESPONSE_CONTENT}
      -s -k
      -w "%{http_code}::%{content_type}::%{size_download}\n"
    )
    if [[ -n $GOAUTH ]] 
    then
        curl_options+=( -H "Authorization: Globus-Goauthtoken $GOAUTH" )
    else
	curl_options+=( -b "$COOKIES" -c "$COOKIES" )
    fi
    curl "${curl_options[@]}" "$@"
}

hex2base64()
{
    # decode stdin hex digits to binary and recode to base64
    xxd -r -p | base64
}

mymd5sum()
{
    # take data on stdin and output base64 encoded hash
    md5sum | sed -e "s/ \+-//" | hex2base64
}

NUM_FAILURES=0
NUM_TESTS=0

BASE_URL=${BASE_URL:-https://$(hostname)/hatrac}

dotest()
{
    pattern="$1"
    url="$2"
    shift 2

    printf "%s " "$@" "${BASE_URL}$url" >&2
    summary=$(mycurl "$@" "${BASE_URL}$url")
    printf " -->  %s " "$summary" >&2

    md5_mismatch=
    hash1=
    hash2=
    if grep -i -q content-md5 ${RESPONSE_HEADERS} && [[ "$1" != '--head' ]]
    then
	hash1=$(grep -i content-md5 ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+: \([A-Za-z0-9/+=]\+\).*/\1/")
	hash2=$(mymd5sum < ${RESPONSE_CONTENT})
	if [[ "$hash1" != "$hash2" ]]
	then
	    md5_mismatch="Content-MD5 ${hash1} mismatch body ${hash2}"
	else
	    echo "Content-MD5 == body md5sum $hash1 == $hash2" >&2
	fi
    fi

    if [[ "$summary" != $pattern ]] || [[ -n "${md5_mismatch}" ]]
    then
	printf "FAILED.\n" >&2
	cat <<EOF
--
${hash1}
--
${hash2}
--
EOF
	cat <<EOF

TEST FAILURE:

Expected result: $pattern
Actual result: $summary
${md5_mismatch}
Response headers:
$(cat ${RESPONSE_HEADERS})
Response body:
$(head -c 500 ${RESPONSE_CONTENT})

EOF
	NUM_FAILURES=$(( ${NUM_FAILURES} + 1 ))
    else
	printf "OK.\n" >&2
	if [[ "$VERBOSE" = "true" ]]
	then
	    cat >&2 <<EOF
Response headers:
$(cat ${RESPONSE_HEADERS})
Response body:
$(cat ${RESPONSE_CONTENT})

EOF
	fi
    fi

    NUM_TESTS=$(( ${NUM_TESTS} + 1 ))
}

# initial state of catalog
dotest "200::application/json::*" /
dotest "404::*::*" /ns-${RUNKEY}

# create some test namespaces
dotest "201::text/uri-list::*" /ns-${RUNKEY} -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "409::*::*" /ns-${RUNKEY}/foo -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2 -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo/bar -X PUT -H "Content-Type: application/x-hatrac-namespace"

# status of test namespaces
dotest "200::application/json::*" /ns-${RUNKEY}/foo
dotest "200::application/json::0" /ns-${RUNKEY}/foo --head
dotest "409::*::*" /ns-${RUNKEY}/foo -X PUT -H "Content-Type: application/json"

# test objects
md5=$(mymd5sum < $0)
script_size=$(stat -c "%s" $0)
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo/obj1 -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "204::*::*" /ns-${RUNKEY}/foo/obj1 -X DELETE
dotest "409::*::*" /ns-${RUNKEY}/foo/obj1 -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "Content-MD5: $md5"
obj1_vers1="$(cat ${RESPONSE_CONTENT})"
obj1_vers1="${obj1_vers1#/hatrac}"
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Type: application/x-bash" \
       -H "Content-MD5: 1B2M2Y8AsgTpgAmY7PhCfg=="  # valid hash of /dev/null will mismatch input data
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Type: application/x-bash" \
       -H "Content-MD5: YmFkX21kNQo="  # valid base64 but invalid MD5
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Type: application/x-bash" \
       -H "Content-MD5: bad_md5"  # invalid base64
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1
obj1_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"
dotest "304::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-None-Match: ${obj1_etag}"
dotest "200::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-Match: ${obj1_etag}"
dotest "304::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-None-Match: *"
dotest "304::*::*" "${obj1_vers1}" -H "If-None-Match: ${obj1_etag}"
dotest "200::application/x-bash::${script_size}" "${obj1_vers1}" -H "If-None-Match: \"wrongetag\""
dotest "304::*::*" "${obj1_vers1}" -H "If-Match: \"wrongetag\""
dotest "200::application/x-bash::0" /ns-${RUNKEY}/foo2/obj1 --head
dotest "200::application/x-bash::${script_size}" "${obj1_vers1}"
dotest "200::application/x-bash::0" "${obj1_vers1}" --head

dotest "200::application/json::[1-9]*" "/ns-${RUNKEY}/foo2/obj1;versions"
dotest "404::*::*" "/ns-${RUNKEY}/foo2;versions"

# test partial GET
dotest "200::*::${script_size}" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=0-"
dotest "206::*::$((${script_size} - 10))" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=10-"
dotest "206::*::891" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=10-900"
dotest "206::*::900" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=-900"
dotest "200::*::${script_size}" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=-900000"

# test partial GET error conditions
dotest "501::*::*" /ns-${RUNKEY}/foo2 -H "Range: bytes=1-2"
dotest "501::*::*" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=1-2,3-5"
dotest "416::*::*" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=900000-"
# syntactically invalid means ignore Range!
dotest "200::*::*" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=900000-5,1-2"

# test deletion
dotest "412::*::*" "${obj1_vers1}" -X DELETE -H "If-None-Match: *"
dotest "412::*::*" "${obj1_vers1}" -X DELETE -H "If-None-Match: ${obj1_etag}"
dotest "204::*::*" "${obj1_vers1}" -X DELETE
dotest "404::*::*" "${obj1_vers1}"
dotest "409::*::*" /ns-${RUNKEY}/foo2/obj1

# test deletion for objects with no current version
dotest "201::*::*" /ns-${RUNKEY}/foo/obj3 -X PUT -T $0 -H "Content-Type: application/x-bash"
obj3_vers1="$(cat ${RESPONSE_CONTENT})"
obj3_vers1="${obj3_vers1#/hatrac}"
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo/obj3
dotest "204::*::*" "${obj3_vers1}" -X DELETE
dotest "404::*::*" "${obj3_vers1}"
dotest "409::*::*" /ns-${RUNKEY}/foo/obj3
dotest "204::*::*" /ns-${RUNKEY}/foo/obj3 -X DELETE
dotest "404::*::*" /ns-${RUNKEY}/foo/obj3

total_bytes=${script_size}
chunk_bytes=1024
upload_file_name="$0"

# test chunk upload (S3 requires at least 5MB chunks)
if [[ $DEPLOYMENT == "amazons3" ]]
then
    upload_file_name="/tmp/dummy-${RUNKEY}"
    chunk_bytes=5242881
    # generate 5MB + file
    dd if=/dev/urandom bs=${chunk_bytes} count=1 2>/dev/null | base64 > ${upload_file_name}
    md5=$(mymd5sum < ${upload_file_name})
    total_bytes=$(stat -c "%s" ${upload_file_name})
fi

cat > ${TEST_DATA} <<EOF
{"chunk_bytes": ${chunk_bytes},
"total_bytes": ${total_bytes},
"content_type": "application/x-bash",
"content_md5": "$md5"}
EOF

# cannot upload to a deleted object
dotest "409::*::*" "/ns-${RUNKEY}/foo/obj1;upload" -T "${TEST_DATA}" -X POST -H "Content-Type: application/json"

# check upload job for new version of existing test object
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj1;upload"  \
    -T "${TEST_DATA}" \
    -X POST \
    -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
dotest "200::application/json::*" "${upload}"
dotest "200::application/json::*" "${upload}" --head
dotest "200::*::*" "/ns-${RUNKEY}/foo2/obj1;upload"
dotest "200::*::*" "/ns-${RUNKEY}/foo2/obj1;upload" --head
dotest "404::*::*" "/ns-${RUNKEY}/foo2;upload"
dotest "405::*::*" "${upload}/0"
dotest "405::*::*" "${upload}/0" --head

split -b ${chunk_bytes} -d ${upload_file_name} /tmp/parts-${RUNKEY}-
for part in /tmp/parts-${RUNKEY}-*
do
    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
    md5=$(mymd5sum < "$part")
    dotest "204::*::*" "${upload}/$pos" -T "$part" -H "Content-MD5: $md5"
done

dotest "409::*::*" "${upload}/$(( ${total_bytes} / ${chunk_bytes} + 2 ))" -T "$part"
dotest "400::*::*" "${upload}/-1" -T "$part"

dotest "201::*::*" "${upload}" -X POST
dotest "404::*::*" "${upload}" -X POST
dotest "200::application/x-bash::*" /ns-${RUNKEY}/foo2/obj1
obj1_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"

# check upload job deletion
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj1;upload" -T "${TEST_DATA}" -X POST -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
dotest "204::*::*" "${upload}" -X DELETE

dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj1;upload" -T "${TEST_DATA}" -X POST -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
split -b ${chunk_bytes} -d ${upload_file_name} /tmp/parts-${RUNKEY}-
for part in /tmp/parts-${RUNKEY}-*
do
    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
    md5=$(mymd5sum < "$part")
    dotest "204::*::*" "${upload}/$pos" -T "$part" -H "Content-MD5: $md5"
    break
done
dotest "204::*::*" "${upload}" -X DELETE

# check upload job for brand new object
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj2;upload"  \
    -T "${TEST_DATA}" \
    -X POST \
    -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
dotest "200::application/json::*" "${upload}"

split -b ${chunk_bytes} -d ${upload_file_name} /tmp/parts-${RUNKEY}-
for part in /tmp/parts-${RUNKEY}-*
do
    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
    md5=$(mymd5sum < "$part")
    dotest "204::*::*" "${upload}/$pos" -T "$part" -H "Content-MD5: $md5"
done

dotest "201::*::*" "${upload}" -X POST
dotest "200::application/x-bash::*" /ns-${RUNKEY}/foo2/obj2

# check upload job for brand new object canceled implicitly by object deletion
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo/obj4;upload"  \
    -T "${TEST_DATA}" \
    -X POST \
    -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
dotest "200::application/json::*" "${upload}"
dotest "204::*::*" /ns-${RUNKEY}/foo/obj4 -X DELETE
dotest "404::*::*" "${upload}"

# check upload job with mismatched MD5
cat > ${TEST_DATA} <<EOF
{"chunk_bytes": ${chunk_bytes},
"total_bytes": ${total_bytes},
"content_type": "application/x-bash",
"content_md5": "$(echo "" | mymd5sum)"}
EOF
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj2bad;upload"  \
    -T "${TEST_DATA}" \
    -X POST \
    -H "Content-Type: application/json"
upload="$(cat ${RESPONSE_CONTENT})"
upload="${upload#/hatrac}"
dotest "200::application/json::*" "${upload}"

split -b ${chunk_bytes} -d ${upload_file_name} /tmp/parts-${RUNKEY}-
for part in /tmp/parts-${RUNKEY}-*
do
    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
    md5=$(mymd5sum < "$part")
    dotest "204::*::*" "${upload}/$pos" -T "$part" -H "Content-MD5: $md5"
done

dotest "409::*::*" "${upload}" -X POST

# check object conditional updates
dotest "412::*::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "If-Match: \"wrongetag\""
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "If-Match: ${obj1_etag}"
obj1_vers2="$(cat ${RESPONSE_CONTENT})"
obj1_vers2="${obj1_vers2#/hatrac}"
dotest "200::application/x-bash::*" /ns-${RUNKEY}/foo2/obj1
vers2_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"
dotest "412::*::*" /ns-${RUNKEY}/foo2/obj1 -X DELETE -H "If-Match: ${obj1_etag}"
dotest "204::*::*" "${obj1_vers2}" -X DELETE -H "If-Match: ${vers2_etag}"
dotest "204::*::*" /ns-${RUNKEY}/foo2/obj1 -X DELETE -H "If-Match: ${obj1_etag}"

# check ACL API
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl"
acl_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"
dotest "304::*::*" "/ns-${RUNKEY}/foo;acl" -H "If-None-Match: ${acl_etag}"
dotest "200::application/json::0" "/ns-${RUNKEY}/foo;acl" --head
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/owner"
dotest "200::application/json::0" "/ns-${RUNKEY}/foo;acl/owner" --head
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/create"
dotest "404::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X PUT
dotest "200::text/plain*::0" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" --head
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X DELETE -H "If-Match: *"
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X PUT
dotest "200::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"
acl_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"
dotest "412::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X DELETE -H "If-None-Match: ${acl_etag}"
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X DELETE -H "If-Match: ${acl_etag}"
dotest "404::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"

# cleanup
dotest "204::*::*" /ns-${RUNKEY} -X DELETE
dotest "404::*::*" /ns-${RUNKEY}


if [[ ${NUM_FAILURES} -gt 0 ]]
then
    echo "FAILED ${NUM_FAILURES} of ${NUM_TESTS} tests" 
    exit 1
else
    echo "ALL ${NUM_TESTS} tests succeeded" >&2
    exit 0
fi
