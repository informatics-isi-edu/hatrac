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
amount of diagnostic output. Setting the environment variable
FAILSTOP=true stops the tests on the first failure.

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

mysha256sum()
{
    # take data on stdin and output base64 encoded hash
    sha256sum | sed -e "s/ \+-//" | hex2base64
}

NUM_FAILURES=0
NUM_TESTS=0

BASE_URL=${BASE_URL:-https://$(hostname)/hatrac}

summarycheck()
{
    if [[ "$summary" != $pattern ]]
    then
	echo "HTTP result does not match expectation ($summary != $pattern)"
	return 1
    fi

    return 0
}

jsonload()
{
    python -c "import sys; import json; v = json.load(sys.stdin);" 2>&1
}

jsonarraydump()
{
    python -c "import sys; import json; v = json.load(sys.stdin); sys.stderr.write('\n'.join(v[:])+'\n');" 2>&1
}

jsoncheck()
{
    if [[ "$summary" == *::application/json::* ]] && [[ "$summary" != *::*::0 ]] && [[ "${request[0]}" != '--head' ]]
    then
	json_error=$(jsonload < ${RESPONSE_CONTENT})
	if [[ $? -ne 0 ]]
	then
	    json_error=${json_error#*ValueError}
	    echo "Error parsing JSON response: ${json_error}"
	    return 1
	fi
	if [[ "$url" != *\;acl* ]]
	then
	    array_content=$(jsonarraydump < ${RESPONSE_CONTENT})
	    if [[ $? -eq 0 ]]
	    then
		urischeck <<< "${array_content}"
		if [[ $? -ne 0 ]]
		then
		    return 1
		fi
	    fi
	fi
    fi
    return 0
}

hashcheck()
{
    # hashcheck header hasher 
    header="$1"
    hasher="$2"
    shift 2
    if grep -i -q ${header} < ${RESPONSE_HEADERS} && [[ "${request[0]}" != '--head' ]]
    then
	hash1=$(grep -i ${header} ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+: \([A-Za-z0-9/+=]\+\).*/\1/")
	hash2=$("$hasher" < ${RESPONSE_CONTENT})
	if [[ "$hash1" != "$hash2" ]]
	then
	    echo "${header} header != body (${hash1} != ${hash2})"
	    return 1
	elif [[ "$VERBOSE" = "true" ]]
	then
	    echo "${header} header == body (${hash1} == ${hash2})" >&2
	fi
    fi
    return 0
}

md5check()
{
    hashcheck content-md5 mymd5sum
}

sha256check()
{
    hashcheck content-sha256 mysha256sum
}

urischeck()
{
    while read uri
    do
	if [[ -n "$uri" ]]
	then
	    if [[ "$uri" != /hatrac/* ]]
	    then
		echo "Response URI '${uri}' lacks /hatrac prefix"
		return 1
	    elif [[ "$uri" == /hatrac/hatrac/* ]]
	    then
		echo "Response URI '${uri}' has doubled /hatrac prefix"
		return 1
	    elif [[ "$VERBOSE" = "true" ]]
	    then
		echo "Response URI '${uri}' has expected /hatrac prefix" >&2
	    fi
	fi
    done
    return 0
}

texturicheck()
{
    if [[ "$summary" == *::text/uri-list::* ]] && [[ "${request[0]}" != "--head" ]]
    then
	urischeck < "${RESPONSE_CONTENT}"
    else
	return 0
    fi
}

dotest()
{
    pattern="$1"
    url="$2"
    shift 2

    request=( "$@" "${BASE_URL}$url" )
    printf "%s " "${request[@]}"  >&2
    summary=$(mycurl "${request[@]}" )

    if [[ "${request[0]}" = '--head' ]]
    then
	# curl summary has download_size 0 when we want to check content-length...
	content_length=$(grep -i 'content-length' < ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+:[[:space:]]*\([0-9]*\).*/\1/")
	[[ -n "${content_length}" ]] || content_length=unknown
	summary="${summary%0}${content_length}"
    fi

    printf " -->  %s " "$summary" >&2

    mismatches=()
    
    for check in summarycheck md5check sha256check jsoncheck texturicheck
    do
	error_text=$( "$check" )
	if [[ $? -ne 0 ]]
	then
	    mismatches+=( "${error_text}" )
	fi
    done
    
    if [[ "${#mismatches[*]}" -gt 0 ]]
    then
	cat >&2 <<EOF
FAILED test $(( ${NUM_TESTS} + 1 )).

$(printf "%s\n" "${mismatches[@]}")

Response headers:
$(cat ${RESPONSE_HEADERS} | sed -e "s/\(.*\)/    \1/")
Response body:
$(head -c 500 ${RESPONSE_CONTENT} | sed -e "s/\(.*\)/    \1/")

EOF
	NUM_FAILURES=$(( ${NUM_FAILURES} + 1 ))
        if [[ -n "${FAILSTOP:=}" ]]
        then
            cat >&2 <<EOF
Exiting due to FAILSTOP environment setting.
EOF
            exit 1
        fi
    else
	cat >&2 <<EOF
OK.
EOF
	if [[ "$VERBOSE" = "true" ]]
	then
	    cat >&2 <<EOF
Response headers:
$(cat ${RESPONSE_HEADERS} | sed -e "s/\(.*\)/    \1/")
Response body:
$(head -c 500 ${RESPONSE_CONTENT} | sed -e "s/\(.*\)/    \1/")

EOF
	fi
    fi

    NUM_TESTS=$(( ${NUM_TESTS} + 1 ))
}

dohdrtest()
{
    # args: hdr sedpatgrp1 expectedval
    gotval=$(grep -i "$1" < ${RESPONSE_HEADERS} | sed -e "s/^[^:]\+:[[:space:]]*${2}.*/\1/")
    printf "%s" "Expect response header $1: $3 (actual $gotval)... " >&2
    if [[ "$gotval" = "$3" ]]
    then
	echo "OK." >&2
    else
	echo "FAILED test $(( ${NUM_TESTS} + 1 ))." >&2
	cat >&2 <<EOF

Response headers:
$(cat ${RESPONSE_HEADERS} | sed -e "s/\(.*\)/    \1/")

EOF
	NUM_FAILURES=$(( ${NUM_FAILURES} + 1 ))
        if [[ -n "${FAILSTOP:=}" ]]
        then
            cat >&2 <<EOF
Exiting due to FAILSTOP environment setting.
EOF
            exit 1
        fi
    fi
    NUM_TESTS=$(( ${NUM_TESTS} + 1 ))
}

# initial state of catalog
dotest "200::application/json::*" /
dotest "404::*::*" /ns-${RUNKEY}
dotest "200::application/json::*" "/?cid=smoke"
dotest "404::*::*" "/ns-${RUNKEY}?cid=smoke"

# create some test namespaces
dotest "201::text/uri-list::*" "/ns-${RUNKEY}?cid=smoke" -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo"       -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "409::*::*"             "/ns-${RUNKEY}/foo"       -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo2"      -X PUT -H "Content-Type: application/x-hatrac-namespace"
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/foo/bar"   -X PUT -H "Content-Type: application/x-hatrac-namespace"

# status of test namespaces
dotest "200::application/json::*" /ns-${RUNKEY}/foo
dotest "200::text/uri-list::*" "/ns-${RUNKEY}/foo" -H "Accept: text/uri-list"
dotest "200::text/html*::*" "/ns-${RUNKEY}/foo" -H "Accept: text/html"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo?cid=smoke" --head
dotest "409::*::*" "/ns-${RUNKEY}/foo?cid=smoke" -X PUT -H "Content-Type: application/json"

# test objects
md5=$(mymd5sum < $0)
sha=$(mysha256sum < $0)
script_size=$(stat -c "%s" $0)

dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo/obj1 -X PUT -T $0 -H "Content-Type: application/x-bash"
obj1_vers0="$(cat ${RESPONSE_CONTENT})"
obj1_vers0="${obj1_vers0#/hatrac}"

# metadata on object-version
dotest "200::application/json::*" "${obj1_vers0};metadata/"
dotest "200::application/json::*" "${obj1_vers0};metadata/?cid=smoke"
dotest "200::application/json::*" "${obj1_vers0};metadata?cid=smoke"

# service assigns content-type automagically
dotest "200::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type"
dotest "200::*::*" "${obj1_vers0};metadata/content-type"
dotest "200::*::*" "${obj1_vers0};metadata/content-type?cid=smoke"

# we can modify content-type
cat > ${TEST_DATA} <<EOF
text/plain
EOF
dotest "204::*::*" "${obj1_vers0};metadata/content-type" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "200::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type"
dotest "200::*::*" "${obj1_vers0};metadata/content-type"
dotest "204::*::*" "${obj1_vers0};metadata/content-type" -X DELETE
dotest "204::*::*" "${obj1_vers0};metadata/content-type?cid=smoke" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "204::*::*" "${obj1_vers0};metadata/content-type?cid=smoke" -X DELETE
dotest "404::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type"
dotest "404::*::*" "${obj1_vers0};metadata/content-type"

# we can modify content-type repeatedly
cat > ${TEST_DATA} <<EOF
application/x-bash
EOF
dotest "204::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "200::text/plain*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type"
dotest "200::application/x-bash::*" "${obj1_vers0}"
dotest "206::application/x-bash::*" "${obj1_vers0}" -H "Range: bytes=2-"
dotest "204::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type" -X DELETE
dotest "404::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-type"
dotest "404::*::*" "${obj1_vers0};metadata/content-type"

# checksums can be applied once and are immutable
dotest "404::*::*" "${obj1_vers0};metadata/content-md5"
dotest "404::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-md5"
cat > ${TEST_DATA} <<EOF
$(mymd5sum < $0)
EOF
dotest "204::*::*" "${obj1_vers0};metadata/content-md5" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "204::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-md5" -T ${TEST_DATA} -H "Content-Type: text/plain"
cat > ${TEST_DATA} <<EOF
$(echo "" | mymd5sum)
EOF
dotest "409::*::*" "${obj1_vers0};metadata/content-md5" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "409::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-md5" -T ${TEST_DATA} -H "Content-Type: text/plain"

dotest "404::*::*" "${obj1_vers0};metadata/content-sha256"
dotest "404::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-sha256"
cat > ${TEST_DATA} <<EOF
$(mysha256sum < $0)
EOF
dotest "204::*::*" "${obj1_vers0};metadata/content-sha256" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "204::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-sha256" -T ${TEST_DATA} -H "Content-Type: text/plain"
cat > ${TEST_DATA} <<EOF
$(echo "" | mysha256sum)
EOF
dotest "409::*::*" "${obj1_vers0};metadata/content-sha256" -T ${TEST_DATA} -H "Content-Type: text/plain"
dotest "409::*::*" "/ns-${RUNKEY}/foo/obj1;metadata/content-sha256" -T ${TEST_DATA} -H "Content-Type: text/plain"

# test path with escaped unicode name "ǝɯɐuǝlᴉɟ ǝpoɔᴉun"
TEST_DISPOSITION="filename*=UTF-8''%C7%9D%C9%AF%C9%90u%C7%9Dl%E1%B4%89%C9%9F%20%C7%9Dpo%C9%94%E1%B4%89un"

dotest "204::*::*" /ns-${RUNKEY}/foo/obj1 -X DELETE
dotest "404::*::*" "/ns-${RUNKEY}/foo/obj1?cid=smoke" -X DELETE
dotest "409::*::*" /ns-${RUNKEY}/foo/obj1 -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "201::text/uri-list::*" /ns-${RUNKEY}/foo2/obj1 \
    -X PUT -T $0 \
    -H "Content-Type: application/x-bash" \
    -H "Content-MD5: $md5" \
    -H "Content-SHA256: $sha" \
    -H "Content-Disposition: ${TEST_DISPOSITION}"
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
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Disposition: filename*=UTF-8''not escaped.txt" # space char must be escaped
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Disposition: filename*=UTF-8''illegal%2Fslash.txt" # slash not allowed
dotest "400::*::*" /ns-${RUNKEY}/foo2/obj1_bad \
       -X PUT -T $0 \
       -H "Content-Disposition: filename*=UTF-8''illegal%5Cslash.txt" # slash not allowed
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1
obj1_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"
dotest "304::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-None-Match: ${obj1_etag}"
dotest "200::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-Match: ${obj1_etag}"
dotest "304::*::*" /ns-${RUNKEY}/foo2/obj1 -H "If-None-Match: *"
dotest "304::*::*" "${obj1_vers1}" -H "If-None-Match: ${obj1_etag}"
dotest "200::application/x-bash::${script_size}" "${obj1_vers1}" -H "If-None-Match: \"wrongetag\""
dotest "304::*::*" "${obj1_vers1}" -H "If-Match: \"wrongetag\""
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1 --head

dotest "200::application/x-bash::${script_size}" "${obj1_vers1}"
dohdrtest 'content-disposition' "\([-_*='.~A-Za-z0-9%]\+\)" "${TEST_DISPOSITION}"
dohdrtest 'content-location' "\([^[:space:]]\+\)" "/hatrac${obj1_vers1}"
dotest "200::application/x-bash::${script_size}" "${obj1_vers1}" --head
dohdrtest 'content-location' "\([^[:space:]]\+\)" "/hatrac${obj1_vers1}"
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1
dohdrtest 'content-location' "\([^[:space:]]\+\)" "/hatrac${obj1_vers1}"
dotest "200::application/x-bash::${script_size}" /ns-${RUNKEY}/foo2/obj1 --head
dohdrtest 'content-location' "\([^[:space:]]\+\)" "/hatrac${obj1_vers1}"

dotest "200::application/json::[1-9]*" "/ns-${RUNKEY}/foo2/obj1;versions"
dotest "200::application/json::[1-9]*" "/ns-${RUNKEY}/foo2/obj1;versions?cid=smoke"
dotest "404::*::*" "/ns-${RUNKEY}/foo2;versions"
dotest "200::text/html*::[1-9]*" "/ns-${RUNKEY}/foo2/obj1;versions" -H "Accept: text/html"

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
dotest "416::*::*" "/ns-${RUNKEY}/foo2/obj1?cid=smoke" -H "Range: bytes=900000-"
# syntactically invalid means ignore Range!
dotest "200::*::*" /ns-${RUNKEY}/foo2/obj1 -H "Range: bytes=900000-5,1-2"

# test deletion
dotest "412::*::*" "${obj1_vers1}" -X DELETE -H "If-None-Match: *"
dotest "412::*::*" "${obj1_vers1}" -X DELETE -H "If-None-Match: ${obj1_etag}"
dotest "204::*::*" "${obj1_vers1}" -X DELETE
dotest "404::*::*" "${obj1_vers1}?cid=smoke" -X DELETE
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

# test ancestor conflict modes
dotest "404::*::*" "/ns-${RUNKEY}/not1/obj1"        -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "404::*::*" "/ns-${RUNKEY}/not2/there2/obj1" -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/not1/obj1?parents=true"        -X PUT -T $0 -H "Content-Type: application/x-bash"
dotest "201::text/uri-list::*" "/ns-${RUNKEY}/not2/there2/obj1?parents=true" -X PUT -T $0 -H "Content-Type: application/x-bash"

# test chunk upload (S3 requires at least 5MB chunks)
upload_file_name="/tmp/dummy-${RUNKEY}"
chunk_bytes=5242889
# generate 5MB + file
dd if=/dev/urandom bs=${chunk_bytes} count=1 2>/dev/null | base64 > ${upload_file_name}
upload_md5=$(mymd5sum < ${upload_file_name})
upload_sha=$(mysha256sum < ${upload_file_name})
upload_total_bytes=$(stat -c "%s" ${upload_file_name})
split -b ${chunk_bytes} -d ${upload_file_name} /tmp/parts-${RUNKEY}-

# namespaces don't have upload resources
dotest "404::*::*" "/ns-${RUNKEY}/foo2;upload"

upload_query=

douploadtest()
{
    # args: url md5 sha256 jobpat chunk0pat chunk1pat finalpat
    _url="$1"
    _md5="$2"
    _sha256="$3"
    shift 3

    fields=(
	'"content-length": '"${upload_total_bytes}"
	'"content-type": "application/x-bash"'
	'"content-disposition": "'"${TEST_DISPOSITION}"'"'
    )

    if [[ -n "${_md5}" ]]
    then
	fields+=(
	    '"content-md5": "'"${_md5}"'"'
	)
    fi

    if [[ -n "${_sha256}" ]]
    then
	fields+=(
	    '"content-sha256": "'"${_sha256}"'"'
	)
    fi
    
    cat > ${TEST_DATA} <<EOF
{
  $(printf "%s,\n" "${fields[@]}")
  "chunk-length": ${chunk_bytes}
}
EOF

    [[ -n "${upload_query}" ]] && _suffix="?${upload_query}&cid=smoke" || _suffix='?cid=smoke'
    
    dotest "$1" "${_url};upload${_suffix}"  \
	   -T "${TEST_DATA}" \
	   -X POST \
	   -H "Content-Type: application/json"

    upload="$(cat ${RESPONSE_CONTENT})"
    upload="${upload#/hatrac}"

    case "$1" in
	201::*)
	    shift

	    dotest "200::application/json::*" "${upload}"
	    dotest "200::application/json::*" "${upload}" --head
	    dotest "200::application/json::*" "${upload}?cid=smoke"
	    dotest "200::application/json::*" "${upload}?cid=smoke" --head
	    dotest "200::*::*" "${_url};upload"
	    dotest "200::*::*" "${_url};upload" --head
	    dotest "405::*::*" "${upload}/0"
	    dotest "405::*::*" "${upload}/0" --head

	    for part in /tmp/parts-${RUNKEY}-*
	    do
		if [[ $# -gt 0 ]]
		then
		    pos=$(echo "$part" | sed -e "s|/tmp/parts-${RUNKEY}-0*\([0-9]\+\)|\1|")
		    dotest "$1" "${upload}/$pos" -T "$part" -H "Content-MD5: $(mymd5sum < "$part")"
		    dotest "$1" "${upload}/$pos?cid=smoke" -T "$part" -H "Content-MD5: $(mymd5sum < "$part")"
		    shift
		else
		    return 0
		fi
	    done

	    if [[ $# -gt 0 ]]
	    then
		dotest "$1" "${upload}" -X POST
		shift
	    fi
	    ;;
    esac
}

# cannot upload to a deleted object
douploadtest "/ns-${RUNKEY}/foo/obj1" "" "" "409::*::*" 

# check upload job for new version of existing test object... omit finalpat so we can intersperse tests
douploadtest "/ns-${RUNKEY}/foo2/obj1" "${upload_md5}" "${upload_sha}" "201::text/uri-list::*" "204::*::*" "204::*::*"

# test upload listing API
dotest "200::application/json::*" "/ns-${RUNKEY}/foo2/obj1;upload"
dotest "200::text/uri-list::*" "/ns-${RUNKEY}/foo2/obj1;upload" -H "Accept: text/uri-list"
dotest "200::text/html*::*" "/ns-${RUNKEY}/foo2/obj1;upload" -H "Accept: text/html"

# do some bad chunk upload tests before we finalize
dotest "409::*::*" "${upload}/$(( ${upload_total_bytes} / ${chunk_bytes} + 2 ))" -T "$part"
dotest "400::*::*" "${upload}/-1" -T "$part"

# finalize manually and check corner cases
dotest "201::*::*" "${upload}" -X POST
dotest "404::*::*" "${upload}" -X POST

# check finalized object
dotest "200::application/x-bash::${upload_total_bytes}" /ns-${RUNKEY}/foo2/obj1
obj1_etag="$(grep -i "^etag:" < ${RESPONSE_HEADERS} | sed -e "s/^[Ee][Tt][Aa][Gg]: *\(\"[^\"]*\"\).*/\1/")"

# test ancestor conflict modes
douploadtest "/ns-${RUNKEY}/not3/obj1"        "" "" "404::*::*" 
douploadtest "/ns-${RUNKEY}/not4/there2/obj1" "" "" "404::*::*" 
upload_query='parents=true'
douploadtest "/ns-${RUNKEY}/not3/obj1"        "" "" "201::text/uri-list::*" "204::*::*" "204::*::*"
douploadtest "/ns-${RUNKEY}/not4/there2/obj1" "" "" "201::text/uri-list::*" "204::*::*" "204::*::*"
upload_query=''


# check upload job deletion
douploadtest "/ns-${RUNKEY}/foo2/obj1" "${upload_md5}" "${upload_sha}" "201::text/uri-list::*" "204::*::*" "204::*::*"
dotest "204::*::*" "${upload}" -X DELETE

douploadtest "/ns-${RUNKEY}/foo2/obj1" "${upload_md5}" "" "201::text/uri-list::*"
dotest "204::*::*" "${upload}?cid=smoke" -X DELETE

# check upload job for brand new object
douploadtest "/ns-${RUNKEY}/foo2/obj2" "${upload_md5}" "" "201::text/uri-list::*" "204::*::*" "204::*::*" "201::*::*"
dotest "200::application/x-bash::${upload_total_bytes}" /ns-${RUNKEY}/foo2/obj2

# check upload job for brand new object canceled implicitly by object deletion
douploadtest "/ns-${RUNKEY}/foo/obj4" "${upload_md5}" "" "201::text/uri-list::*" "204::*::*" "204::*::*"
dotest "200::application/json::*" "${upload}"
dotest "204::*::*" /ns-${RUNKEY}/foo/obj4 -X DELETE
dotest "404::*::*" "${upload}"

# check upload job with mismatched, invalid MD5, invalid base64
douploadtest "/ns-${RUNKEY}/foo2/obj2bad" "$(echo "" | mymd5sum)" "" "201::text/uri-list::*" "204::*::*" "204::*::*" "409::*::*"
douploadtest "/ns-${RUNKEY}/foo2/obj2bad" "YmFkX21kNQo=" "" "400::*::*"
douploadtest "/ns-${RUNKEY}/foo2/obj2bad" "bad_md5" "" "400::*::*"

# check upload job with mismatched, invalid MD5, invalid base64 in final chunk
douploadtest "/ns-${RUNKEY}/foo2/obj2bad" "${upload_md5}" "" "201::text/uri-list::*" "204::*::*" "204::*::*"
parts=( /tmp/parts-${RUNKEY}-* )
dotest "400::*::*" "${upload}/0" -T "${parts[0]}" -H "Content-MD5: $(echo "" | mymd5sum)"
dotest "400::*::*" "${upload}/0" -T "${parts[0]}" -H "Content-MD5: YmFkX21kNQo="
dotest "400::*::*" "${upload}/0" -T "${parts[0]}" -H "Content-MD5: bad_md5"

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
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl" --head
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/owner"
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/owner" --head
dotest "200::application/json::*" "/ns-${RUNKEY}/foo;acl/create"
dotest "404::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY"
dotest "204::*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" -X PUT
dotest "200::text/plain*::*" "/ns-${RUNKEY}/foo/bar;acl/create/DUMMY" --head
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
