#!/bin/bash

# Run basic Hatrac REST API tests 

COOKIES=${COOKIES:-cookie}

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

[[ -r "$COOKIES" ]] || error 

RUNKEY=smoketest-$RANDOM
RESPONSE_HEADERS=/tmp/${RUNKEY}-response-headers
RESPONSE_CONTENT=/tmp/${RUNKEY}-response-content
TEST_DATA=/tmp/${RUNKEY}-test-data

cleanup()
{
    rm -f ${RESPONSE_HEADERS} ${RESPONSE_CONTENT} ${TEST_DATA}
    rm -f /tmp/parts-${RUNKEY}*
}

trap cleanup 0

mycurl()
{
    touch ${RESPONSE_HEADERS}
    touch ${RESPONSE_CONTENT}
    truncate -s 0 ${RESPONSE_HEADERS}
    truncate -s 0 ${RESPONSE_CONTENT}
    curl -D ${RESPONSE_HEADERS} \
	-o ${RESPONSE_CONTENT} \
	-s \
	-k -b "$COOKIES" -c "$COOKIES" \
	-w "%{http_code}::%{content_type}::%{size_download}\n" \
	"$@"
}

NUM_FAILURES=0
NUM_TESTS=0

BASE_URL="https://$(hostname)/hatrac"

dotest()
{
    pattern="$1"
    url="$2"
    shift 2

    printf "%s " "$@" "${BASE_URL}$url" >&2
    summary=$(mycurl "$@" "${BASE_URL}$url")
    printf " -->  %s " "$summary" >&2

    md5_mismatch=
    if grep -i -q content-md5 ${RESPONSE_HEADERS}
    then
	hash1=$(grep -i content-md5 ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+: \([a-z0-9]\+\).*/\1/")
	hash2=$(md5sum < ${RESPONSE_CONTENT} | sed -e "s/ \+-//")
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
$(cat ${RESPONSE_CONTENT})

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
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2 -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo/bar -X PUT -H "Content-Type: application/x-hatrac-namespace"

# status of test namespaces
dotest "200::application/json::*" /ns-${RUNKEY}/foo
dotest "405::*::*" /ns-${RUNKEY}/foo -X PUT -H "Content-Type: application/x-hatrac-namespace"

# test objects
md5=$(md5sum < $0 | sed -e "s/ \+-//")
script_size=$(stat -c "%s" $0)
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "Content-MD5: $md5"
obj1_vers1="$(cat ${RESPONSE_CONTENT})"
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1
dotest "200::application/x-bash::${script_size}" "${obj1_vers1}"

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
dotest "204::*::*" "${obj1_vers1}" -X DELETE
dotest "404::*::*" "${obj1_vers1}"
dotest "409::*::*" /ns-${RUNKEY}/foo2/obj1

# test chunk upload
cat > ${TEST_DATA} <<EOF
{"chunk_bytes": 1024,
 "total_bytes": ${script_size},
 "content_type": "application/x-bash",
 "content_md5": "$md5"}
EOF
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj1;upload"  \
    -T "${TEST_DATA}" \
    -X POST \
    -H "Content-Type: application/json"
upload="$(cat "${RESPONSE_CONTENT}")"
split -b 1024 -d "$0" /tmp/parts-${RUNKEY}-
for part in /tmp/parts-${RUNKEY}-*
do
    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
    md5=$(md5sum < "$part" | sed -e "s/ \+-//")
    dotest "204::*::*" "${upload}/$pos" -T "$part" -H "Content-MD5: $md5"
done
dotest "201::*::*" "${upload}" -X POST
dotest "404::*::*" "${upload}" -X POST
dotest "200::application/x-bash::*" /ns-${RUNKEY}/foo2/obj1

# check ACL API
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/owner"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/create"
dotest "404::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X PUT
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X DELETE
dotest "404::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"

# check safety features
dotest "[45]??::*::*" "/ns-${RUNKEY}/" -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "[45]??::*::*" "/ns-${RUNKEY}/." -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "[45]??::*::*" "/ns-${RUNKEY}/.." -X PUT -H "Content-Type: application/x-hatrac-namespace"

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
