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

# Fetching ginkgo for running the test
export GO111MODULE=on
if ! (go mod vendor && go install github.com/onsi/ginkgo/ginkgo)
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

ginkgo -mod=mod "${OPTS[@]}" --focus="$FOCUS" tests/e2e

# Checking for test status
TEST_PASS=$?
if [[ $TEST_PASS -ne 0 ]]; then
    exit 1
fi

