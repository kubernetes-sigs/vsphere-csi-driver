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
set -o xtrace

go version

# Change directories to the parent directory of the one in which this
# script is located.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

go install honnef.co/go/tools/cmd/staticcheck@2023.1

GOOS=linux "$(go env GOPATH)"/bin/staticcheck --version

# shellcheck disable=SC2046
# shellcheck disable=SC1083
GOOS=linux "$(go env GOPATH)"/bin/staticcheck $(go list ./... | grep -v /vendor/)
