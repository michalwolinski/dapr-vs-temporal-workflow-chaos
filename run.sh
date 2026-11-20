#!/usr/bin/env bash
# Runs the tests inside a pinned Maven + JDK 25 image; Testcontainers starts the engines next to it through the
# host's Docker socket. Pass Maven arguments to narrow it down:
#   ./run.sh                     all tests, writes test-results.log
#   ./run.sh -Dtest=CamundaTest  one class
# The project is streamed into the container instead of mounted, so a file sharing cache cannot hand the build an
# old copy of a source file.
set -euo pipefail
cd "$(dirname "$0")"
MAVEN_IMAGE='maven:3.9-eclipse-temurin-25@sha256:93b8a14ea2f412782e4e842651273b4d903e35cc496284f178fbbe2d67d00976'
COPYFILE_DISABLE=1 tar --exclude=./target --exclude=./.git -cf - . |
    docker run --rm -i \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v dapr-vs-temporal-m2:/root/.m2 \
        -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
        -e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
        "$MAVEN_IMAGE" \
        bash -c 'mkdir /src && tar --warning=no-unknown-keyword -xf - -C /src && cd /src && exec mvn -B -ntp test "$@"' -- "$@" 2>&1 |
    tee test-results.log
exit "${PIPESTATUS[1]}"
