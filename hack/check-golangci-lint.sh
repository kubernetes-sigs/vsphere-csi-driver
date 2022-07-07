#!/bin/bash

# Copyright 2020 The Kubernetes Authors.
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

# Change directories to the parent directory of the one in which this
# script is located.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

usage() {
  cat <<EOF
usage: ${0} [FLAGS]
  Lints the project's go files.

FLAGS
  -d    use the docker image
  -h    prints this help screen
EOF
}

while getopts ':dh' opt; do
  case "${opt}" in
  d)
    DO_DOCKER=1
    ;;
  h)
    usage 1>&2; exit 1
    ;;
  \?)
    { echo "invalid option: -${OPTARG}"; usage; } 1>&2; exit 1
    ;;
  :)
    echo "option -${OPTARG} requires an argument" 1>&2; exit 1
    ;;
  esac
done
shift $((OPTIND-1))

export GOOS=linux
if [ ! "${DO_DOCKER-}" ]; then
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)"/bin v1.46.2
  "$(go env GOPATH)"/bin/golangci-lint run -v --timeout=1200s
else
  docker run --rm -v "$(pwd)":/app -w /app golangci/golangci-lint:v1.46.2 golangci-lint run -v --timeout=1200s
fi
