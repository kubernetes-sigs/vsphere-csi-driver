#!/bin/bash

# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

# TEMPORARY DIAGNOSTICS -- remove after confirming the k8s.io/kubernetes
# version mismatch seen in the 2026-09-07 CI run (build resolved v1.36.2
# despite tests/e2e/go.mod pinning v1.36.0 via a replace directive).
echo "=== DEBUG: go env ==="
go version
go env GOFLAGS GOPROXY GOSUMDB GOPATH GOMODCACHE
echo "=== DEBUG: go.mod pin for k8s.io/kubernetes ==="
grep -n "k8s.io/kubernetes " tests/e2e/go.mod || true
echo "=== DEBUG: module cache entries for k8s.io/kubernetes ==="
ls -la "$(go env GOMODCACHE)"/k8s.io/*ubernetes* 2>/dev/null || echo "(no cache entries found)"
echo "=== DEBUG: resolved module versions (tests/e2e) ==="
(cd tests/e2e && go list -m k8s.io/kubernetes github.com/container-storage-interface/spec) || true
echo "=== END DEBUG ==="

# Fetching ginkgo for running the test
export GO111MODULE=on
export ACK_GINKGO_DEPRECATIONS=2.27.3
if ! (go mod vendor && go install github.com/onsi/ginkgo/v2/ginkgo@v2.27.3)
then
    echo "go mod vendor or go install ginkgo error"
    exit 1
fi

# Add GOPATH/BIN to PATH
PATH=$PATH:$(go env GOPATH)/bin

# Exporting KUBECONFIG path if not set
if [ -z "${KUBECONFIG-}" ]; then
    export KUBECONFIG=$HOME/.kube/config
fi

# Running the e2e test.
# If $GINKGO_FOCUS not set, run "csi-block-vanilla" by default.
FOCUS=${GINKGO_FOCUS:-}
if [ -z "$FOCUS" ]
then
    FOCUS="csi-block-vanilla"
fi

OPTS=()

if [ -z "${GINKGO_OPTS-}" ]; then
    OPTS=(-v)
else
    read -ra OPTS <<< "-v $GINKGO_OPTS"
fi

OPTS+=("-timeout=24h")
if [ "$FOCUS" == "csi-block-vanilla" ]
then
    ginkgo -mod=mod "${OPTS[@]}" --focus="csi-block-vanilla-destructive" tests/e2e
    # Checking for destructive test status
    TEST_PASS=$?
    if [[ $TEST_PASS -ne 0 ]]; then
        exit 1
    fi
    ginkgo -mod=mod "${OPTS[@]}" --focus="csi-block-vanilla-serialized" tests/e2e
    # Checking for serialized test status
    TEST_PASS=$?
    if [[ $TEST_PASS -ne 0 ]]; then
        exit 1
    fi
    OPTS+=(-p)
    ginkgo -mod=mod "${OPTS[@]}" --focus="csi-block-vanilla-parallelized" tests/e2e
elif [ "$FOCUS" == "csi-block-vanilla-parallelized" ]
then
    OPTS+=(-p)
    ginkgo -mod=mod "${OPTS[@]}" --focus="csi-block-vanilla-parallelized" tests/e2e
else
    ginkgo -mod=mod "${OPTS[@]}" --focus="$FOCUS" -r tests/e2e
fi

# Checking for test status
TEST_PASS=$?
if [[ $TEST_PASS -ne 0 ]]; then
    exit 1
fi

