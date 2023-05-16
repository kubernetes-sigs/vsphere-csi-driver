#!/bin/bash

# Copyright 2023 VMware, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SLACK_WEBHOOK_URL="${CSI_SLACK_WEBHOOK_URL}"
USERNAME="${GITLAB_USER_NAME}"
ICON_EMOJI=":robot_face:"
CHANNEL="#${CHANNEL_TEST}"
MAIN_REASON="Release testbed API failed"
REASON="curl -X 'PUT' -H 'accept: application/json' -u ${TESTBED_POOL_SERVICE_BASIC_AUTH_USER}:${TESTBED_POOL_SERVICE_BASIC_AUTH_PASSWORD} ${TESTBED_POOL_SERVICE_ENDPOINT}/v1/pool/${TESTBED_POOL_ID}/releaseTestbed/${TESTBED_ID}"

JOB_URL="${CI_PROJECT_URL}/-/jobs/${CI_JOB_ID}"
COMMIT_URL="${CI_PROJECT_URL}/-/commit/${CI_COMMIT_SHA}"

JOB_DETAILS="*Job Details:*\n \
Job Name: ${CI_JOB_NAME}\n \
Job URL: ${JOB_URL}\n \
Commit URL: ${COMMIT_URL}\n \
Commit Message: ${CI_COMMIT_MESSAGE}\n \
Commit Author: ${GITLAB_USER_NAME}\n \
Remedy : ${REASON}" 

payload="payload={
    \"channel\": \"${CHANNEL}\",
    \"username\": \"${USERNAME}\",
    \"icon_emoji\": \"${ICON_EMOJI}\",
    \"attachments\": [
        {
            \"color\": \"danger\",
            \"text\": \"${MAIN_REASON}\",
            \"fields\": [
                {
                    \"title\": \"Pipeline Details\",
                    \"value\": \"${JOB_DETAILS}\",
                    \"short\": false
                }
            ]
        }
    ]
}"

curl -s -d "${payload}" "${SLACK_WEBHOOK_URL}" > /dev/null
