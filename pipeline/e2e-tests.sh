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

vcIp=$(grep vcIp ./env.json | cut -d = -f2)
vimUsername=$(grep vimUsername ./env.json | cut -d = -f2)
vimPassword=$(grep vimPassword ./env.json | cut -d = -f2)
vcRootPassword=$(grep vcRootPassword ./env.json | cut -d = -f2)

kubeconfigContent=$(cat ./sv_kubeconfig_content.yaml)
echo "$kubeconfigContent" > sv_kubeconfig_content.yaml
kubeconfigPath="$(pwd)/sv_kubeconfig_content.yaml"

echo "$vcIp"
echo "$vcRootPassword" > vc_pwd

export GOVC_INSECURE=1
export GOVC_URL="https://$vimUsername:$vimPassword@$vcIp"

DATACENTER=$(govc datacenter.info | grep -i path | awk '{print $2}')
export DATACENTER=$DATACENTER

COMPUTE_CLUSTER_NAME=$(govc namespace.cluster.ls | awk -F'/' '{print $5}')
export COMPUTE_CLUSTER_NAME=$COMPUTE_CLUSTER_NAME

export E2E_TEST_CONF_FILE=$CI_BUILDS_DIR/e2eTest.conf

echo "$GOVC_URL"
echo "$DATACENTER"
echo "$COMPUTE_CLUSTER_NAME"
echo "$E2E_TEST_CONF_FILE"

tee "$E2E_TEST_CONF_FILE" >/dev/null <<EOF
[Global]
insecure-flag = "true"
hostname = "$vcIp"
user = "$vimUsername"
password = "$vimPassword"
port = "443"
datacenters = "$DATACENTER"
EOF

echo "*******"
govc datacenter.info | grep -i path | awk '{print $2}'
govc namespace.cluster.ls | awk -F'/' '{print $5}'
echo "*******"

export KUBECONFIG="$kubeconfigPath"
export E2E_TEST_CONF_FILE="$E2E_TEST_CONF_FILE"
export VC_REBOOT_WAIT_TIME=1020
export USER=root
export VOLUME_OPS_SCALE=5
export CLUSTER_FLAVOR="WORKLOAD"
export STORAGE_POLICY_FOR_SHARED_DATASTORES="shared-ds-policy"
export STORAGE_POLICY_FOR_SHARED_DATASTORES_2="shared-ds-policy-2"
# need to create tagged vm storage policy
export STORAGE_POLICY_FOR_NONSHARED_DATASTORES="non-shared-ds-policy"
export STORAGE_POLICY_WITH_THICK_PROVISIONING=thick-ds-policy
SHARED_VSPHERE_DATASTORE_URL=$(govc datastore.info "$DATACENTER"/datastore/vsanDatastore | grep URL | awk '{print $2}')
[ -z "$SHARED_VSPHERE_DATASTORE_URL" ] && SHARED_VSPHERE_DATASTORE_URL=$(govc datastore.info "$DATACENTER"/datastore/nfs* | grep URL | awk '{print $2}')
export SHARED_VSPHERE_DATASTORE_URL
LIST_NONSHARED_DATASTORE_URL=$(govc datastore.info "$DATACENTER"/datastore/\*local-\* | grep URL | awk '{print $2}')
[ -z "$LIST_NONSHARED_DATASTORE_URL" ] && LIST_NONSHARED_DATASTORE_URL=$(govc datastore.info "$DATACENTER"/datastore/\*datastore\* | grep URL | awk '{print $2}')
NONSHARED_VSPHERE_DATASTORE_URL=$(echo "$LIST_NONSHARED_DATASTORE_URL" | awk '{print $1; exit}')
export NONSHARED_VSPHERE_DATASTORE_URL
export FULL_SYNC_WAIT_TIME=350
export SVC_NAMESPACE="e2e-test-namespace"
export SVC_NAMESPACE_TO_DELETE="e2e-namespace-to-delete"
VM_NAME=$(govc ls "$DATACENTER"/vm | grep vmdktestvm | awk '{ gsub("/vm/"," ");print $2}')
export VM_NAME
export DISK_URL_PATH="https://$vcIp/folder/$VM_NAME/$VM_NAME""_1.vmdk?dcPath=$DATACENTER&dsName=vsanDatastore"
GOPATH=$(go env GOPATH)
export PATH=$PATH:$GOPATH/bin
export GINKGO_OPTS="--no-color --always-emit-ginkgo-writer --progress --trace -p"
echo "GINKGO_FOCUS=${CNS_CSI_GINKGO_FOCUS}"
export GINKGO_FOCUS="${CNS_CSI_GINKGO_FOCUS}"
export BUSYBOX_IMAGE="${BUSYBOX_IMAGE}"

echo "Running E2E tests."
make test-e2e
