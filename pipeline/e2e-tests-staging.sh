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

set +x

export E2E_TEST_CONF_FILE=$CI_BUILDS_DIR/e2eTest.conf

tee "$E2E_TEST_CONF_FILE" >/dev/null <<EOF
[Global]
insecure-flag = "true"
hostname = "${STAGING_HOSTNAME}"
user = "${STAGING_VC_ADMIN_USERNAME}"
password = "${STAGING_VC_PWD}"
port = "${STAGING_VC_PORT}"
datacenters = "${STAGING_DATACENTER}"
EOF

export KUBECONFIG=$CSI_FVT_STAGING_SA
export SUPERVISOR_CLUSTER_KUBE_CONFIG=$CSI_FVT_STAGING_SA
export STORAGE_POLICY_FOR_SHARED_DATASTORES="${STORAGE_POLICY_FOR_SHARED_DATASTORES}"
export SHARED_VSPHERE_DATASTORE_URL="${SHARED_VSPHERE_DATASTORE_URL}"
export NONSHARED_VSPHERE_DATASTORE_URL="${NONSHARED_VSPHERE_DATASTORE_URL}"
export VOLUME_OPS_SCALE=5
export FULL_SYNC_WAIT_TIME=350
export CLUSTER_FLAVOR="WORKLOAD"
export SVC_NAMESPACE="${SVC_NAMESPACE}"
export GINKGO_OPTS="${CNS_CSI_STAGING_GINKGO_OPTS}"
export GINKGO_FOCUS="${CNS_CSI_STAGING_GINKGO_FOCUS}"
export BUSYBOX_IMAGE="${BUSYBOX_IMAGE}"

GOPATH=$(go env GOPATH)
export PATH=$PATH:$GOPATH/bin
echo "Running E2E tests."
make test-e2e
