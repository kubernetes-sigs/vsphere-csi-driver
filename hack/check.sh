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

! /bin/sh -c 'set -o pipefail' >/dev/null 2>&1 || set -o pipefail

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

TEST_DIR="${TEST_DIR:-$(mktemp -d)}"

run_test() {
  _test_name="$(eval echo "${@}")"
  _test_log="$(mktemp)"
  _test_exit_code="$(mktemp)"
  _test_xml="$(mktemp)"
  _start=$(date +%s)
  echo "${_test_name}"
  { eval "${@}" 2>&1; echo "${?}" >"${_test_exit_code}"; } | tee "${_test_log}"
  _test_exit_code="$(cat "${_test_exit_code}")"
  _stopd=$(date +%s)
  _tsecs=$((_stopd-_start))
  printf '<testcase classname="go_test" name="%s" time="%d"' \
         "${_test_name}" "${_tsecs}" >>"${_test_xml}"
  if [ "${_test_exit_code}" -eq "0" ]; then
    printf ' />\n' >>"${_test_xml}"
  else
    { printf '>\n<failure><![CDATA[' && \
      cat "${_test_log}" && \
      printf ']]></failure>\n</testcase>\n'; } >>"${_test_xml}"
  fi
  cp "${_test_xml}" "${TEST_DIR}"
  return "${_test_exit_code}"
}

run_test_suite() {
  xml_file="${1}"; shift
  start_time=$(date +%s)
  while [ "${#}" -gt "0" ]; do
    cmd="${1}"; shift
    # shellcheck disable=2086
    { run_test ${cmd}; } || exit_code="${exit_code:-${?}}"
  done
  end_time=$(date +%s)
  total_secs=$((end_time-start_time))
  test_result_files="$(/bin/ls "${TEST_DIR}")"
  { printf '<?xml version="1.0" encoding="UTF-8"?>\n'; \
    printf '<testsuite time="%d">\n' "${total_secs}"; } >"${xml_file}"
  for f in ${test_result_files}; do
    cat "${TEST_DIR}/${f}" >>"${xml_file}"
    rm -f "${TEST_DIR}/${f}"
  done
  printf '</testsuite>' >>"${xml_file}"
  [ -z "${exit_code}" ] || exit "${exit_code}"
}

export FMT_FLAGS="-d -e -s"
export MAKEFLAGS="${MAKEFLAGS} --silent --no-print-directory"

run_test_suite "${JUNIT_REPORT:-junit_check.xml}" \
               "make fmt" \
               "make vet" \
               "make lint"
