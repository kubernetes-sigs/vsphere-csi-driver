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

set -uo pipefail

# Fetching ginkgo for running the test
export GO111MODULE=on
go mod vendor && go get -u github.com/onsi/ginkgo/ginkgo
if [ $? -ne 0 ]
then
    echo "go mod vendor or go get ginkgo error"
    exit 1
fi

# Exporting KUBECONFIG path
export KUBECONFIG=$HOME/.kube/config

# Running the e2e test.
# If $GINKGO_FOCUS not set, run "csi-vanilla" by default.
FOCUS=${GINKGO_FOCUS:-}
if [ -z "$FOCUS" ]
then
    FOCUS="csi-vanilla"
fi
ginkgo -v --focus="$FOCUS" tests/e2e

# Checking for test status
TEST_PASS=$?
if [[ $TEST_PASS -ne 0 ]]; then
    exit 1
fi
