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

cleanup()
{
    rm -f ${RESPONSE_HEADERS} ${RESPONSE_CONTENT}
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
	hash1=$(grep -i content-md5 ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+: //")
	hash2=$(md5sum < ${RESPONSE_CONTENT} | sed -e "s/ \+-//")
	if [[ $hash1 != $hash2 ]]
	then
	    md5_mismatch="Content-MD5 != body md5sum!  $hash1 != $hash2"
	else
	    echo "Content-MD5 == body md5sum $hash1 == $hash2" >&2
	fi
    fi

    if [[ "$summary" != $pattern ]] || [[ -n "${md5_mismatch}" ]]
    then
	printf "FAILED.\n" >&2
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
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "Content-MD5: $md5"
obj1_vers1="$(cat ${RESPONSE_CONTENT})"
dotest "200::application/x-bash::*" /ns-${RUNKEY}/foo2/obj1
dotest "200::application/x-bash::*" "${obj1_vers1}"
dotest "204::*::*" "${obj1_vers1}" -X DELETE
dotest "404::*::*" "${obj1_vers1}"
dotest "409::*::*" /ns-${RUNKEY}/foo2/obj1


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
