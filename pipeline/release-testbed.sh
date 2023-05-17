#!/bin/bash

# Copyright 2023 The Kubernetes Authors.
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

set -x
set -o errexit

if [ -n "${TESTBED_ID}" ]; then
    status=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "${TESTBED_POOL_SERVICE_ENDPOINT}/v1/pool/${TESTBED_POOL_ID}/releaseTestbed/${TESTBED_ID}" -H 'accept: application/json' -u "${TESTBED_POOL_SERVICE_BASIC_AUTH_USER}:${TESTBED_POOL_SERVICE_BASIC_AUTH_PASSWORD}")

    if [ "$status" != "200" ] && [ "$status" != "409" ]; then
        echo "Release testbed API call failed with HTTP status code $status. Retrying in 60 seconds..."
        retries=0
        while [ "$status" != "200" ] && [ "$status" != "409" ] && [ "$retries" -lt 5 ]; do
            retries=$((retries+1))
            sleep 60
            echo "Retrying (attempt $retries)..."
            status=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "${TESTBED_POOL_SERVICE_ENDPOINT}/v1/pool/${TESTBED_POOL_ID}/releaseTestbed/${TESTBED_ID}" -H 'accept: application/json' -u "${TESTBED_POOL_SERVICE_BASIC_AUTH_USER}:${TESTBED_POOL_SERVICE_BASIC_AUTH_PASSWORD}")
        done
    fi

    if [ "$status" != "200" ] && [ "$status" != "409" ]; then
        echo "Release testbed API call failed after $retries attempts. Testbed may still be locked. Raising an alert."
        bash pipeline/send_slack_notification.sh
        # Fail the stage forcefully to get attention from maintainers
        exit 1
    else
        echo "Release testbed API call succeeded after $retries attempts."
    fi
else
    echo "Testbed ID is empty or null. Skipping release."
fi
