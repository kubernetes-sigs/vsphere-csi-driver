#!/bin/sh

# Copyright 2018 The Kubernetes Authors.
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

# posix compliant
# verified by https://www.shellcheck.net

# Runs make inside a Docker container.

# Change directories to the project's root directory.
# shellcheck disable=2128
if [ -n "${BASH_SOURCE}" ]; then
  # shellcheck disable=2039
  HACK_DIR="$(dirname "${BASH_SOURCE[0]}")"
elif command -v python >/dev/null 2>&1; then
  HACK_DIR="$(python -c "import os; print(os.path.realpath('$(dirname "${0}")'))")"
elif [ -d "../.git" ]; then
  HACK_DIR="$(pwd)"
elif [ -d ".git" ]; then
  HACK_DIR="$(pwd)/hack"
fi
[ -n "${HACK_DIR}" ] || { echo "unable to find project root" 1>&2; exit 1; }
cd "${HACK_DIR}/.." || { echo "unable to cd to project root" 1>&2; exit 1; }

# When in an interactive terminal add the -t flag so Docker inherits
# a pseudo TTY. Otherwords SIGINT does not work to kill the container
# when running this script interactively.
TERM_FLAGS="-i"
echo "${-}" | grep -q i && TERM_FLAGS="${TERM_FLAGS}t"

# shellcheck disable=2086
docker run --rm ${TERM_FLAGS} ${DOCKER_OPTS} \
  -v "$(pwd)":/build:z \
  -w /build \
  "${CI_IMAGE:-gcr.io/cloud-provider-vsphere/ci:latest}" \
  make "${@}"
