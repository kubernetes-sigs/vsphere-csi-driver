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

if [[ -z "${VSPHERE_CSI_CONTROLLER_IMAGE}" ]]
then
	echo "Env variable unset: VSPHERE_CSI_CONTROLLER_IMAGE."
	exit 1
fi

if [[ -z "${VSPHERE_SYNCER_IMAGE}" ]]
then
	echo "Env variable unset: VSPHERE_SYNCER_IMAGE."
	exit 1
fi

# Borrow a testbed from CSI Testbed Pool Svc.
if ! testbed=$(curl -X 'PUT' "${CNS_TESTBEDPOOL_SVC_URL}" -H 'accept: application/json' -u "${CNS_MANAGER_USERNAME}:${CNS_MANAGER_PASSWORD}");
then
	echo "Unable to borrow a testbed."
	exit 1
fi

echo "TestbedInfo: $testbed ."

# Extract testbed ID from borrow testbed API Response.
if ! id=$(echo "$testbed" | jq '.id'|tr -d '"');
then
	echo "Error getting the id."
	exit 1
fi

# Store the testbed ID in a local file on the workspace.
echo "$id" > pipeline/placeholder.id

# Extract VC IP from borrow testbed API Response.
if ! vcIp=$(echo "$testbed" | jq '.vcIp'|tr -d '"');
then
	echo "Error getting the vcIp."
	exit 1
fi

# Extract External Gateway VM IP from borrow testbed API Response.
if ! externalVMGatewayIp=$(echo "$testbed" | jq '.externalVMGatewayIp'|tr -d '"');
then
	echo "Error getting the externalVMGatewayIp."
	exit 1
fi


# Extract VC root password from borrow testbed API Response.
if ! vcRootPassword=$(echo "$testbed" | jq '.vcRootPassword'|tr -d '"');
then
	echo "Error getting the vcRootPassword."
	exit 1
fi

# Extract VC username from borrow testbed API Response.
if ! vimUsername=$(echo "$testbed" | jq '.vimUsername'|tr -d '"');
then
	echo "Error getting the vimUsername."
	exit 1
fi

# Extract VC Admin password from borrow testbed API Response.
if ! vimPassword=$(echo "$testbed" | jq '.vimPassword'|tr -d '"');
then
	echo "Error getting the vimPassword."
	exit 1
fi

# Print all the values into Console.
echo "VSPHERE_CSI_CONTROLLER_IMAGE = $VSPHERE_CSI_CONTROLLER_IMAGE"
echo "VSPHERE_SYNCER_IMAGE = $VSPHERE_SYNCER_IMAGE"

# Store all the values into Artifacts.
{ echo "id=$id"; echo "vcIp=$vcIp"; echo "vcRootPassword=$vcRootPassword"; echo "vimPassword=$vimPassword"; echo "vimUsername=$vimUsername"; echo "externalVMGatewayIp=$externalVMGatewayIp";} >> ./env.json
# echo "vcIp=$vcIp" >> ./env.json
# echo "vcRootPassword=$vcRootPassword" >> ./env.json
# echo "vimPassword=$vimPassword" >> ./env.json
# echo "vimUsername=$vimUsername" >> ./env.json
# echo "externalVMGatewayIp=$externalVMGatewayIp" >> ./env.json

SV_KUBECONFIG=/tmp/$$

echo "$testbed" | jq '.kubeConfig'|tr -d '"'|base64 -d > $SV_KUBECONFIG

export KUBECONFIG=$SV_KUBECONFIG

# Pod status on testbed before patching the CSI Images
kubectl get pods -n vmware-system-csi

K8S_MAJOR_VERSION=$(kubectl version -o json | jq .serverVersion.major | tr -d '"')
K8S_MINOR_VERSION=$(kubectl version -o json | jq .serverVersion.minor | tr -d '"')

echo "K8S_MAJOR_VERSION=$K8S_MAJOR_VERSION" >> build.env
echo "K8S_MINOR_VERSION=$K8S_MINOR_VERSION" >> build.env

# Replace the kubernetes version in kustomization.yaml
sed -i "s#- ../../manifests/supervisorcluster/1.22#- ../../manifests/supervisorcluster/$K8S_MAJOR_VERSION.$K8S_MINOR_VERSION#g" pipeline/dev/kustomization.yaml

echo "cat pipeline/dev/kustomization.yaml"
cat pipeline/dev/kustomization.yaml
 
if ! kustomize build pipeline/dev | envsubst | kubectl apply -f -;
then
	echo "Error patching the CSI images in the testbed."
	exit 1
fi

# Sleep for 60 seconds so that k8s can act on the applied changes.
echo "Sleeping for 60 seconds..."
sleep 60

# Wait for 2 mins for the CSI deployment to be ready.
if ! kubectl wait deployment -n vmware-system-csi vsphere-csi-controller --for=jsonpath="{.status.readyReplicas}"=3 --timeout=120s;
then
	echo "CSI deployment is not ready within 120s."
	exit 1
fi

# Pod status on testbed after patching the CSI Images
kubectl get pods -n vmware-system-csi

echo "Successfully patched the CSI images."
