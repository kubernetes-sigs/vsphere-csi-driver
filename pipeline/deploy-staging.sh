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
	echo "Env variable unset: VSPHERE_CSI_CONTROLLER_IMAGE"
	exit 1
fi

if [[ -z "${VSPHERE_SYNCER_IMAGE}" ]]
then
	echo "Env variable unset: VSPHERE_SYNCER_IMAGE"
	exit 1
fi

if [[ -z "${CNS_CSI_STAGING_REPO}" ]]
then
	echo "Env variable unset: CNS_CSI_STAGING_REPO"
	exit 1
fi

git clone "$CNS_CSI_STAGING_REPO" || exit 1

# Update the CNS-CSI manifest files to capture any changes.
cp -R manifests/supervisorcluster/* staging-cd/ || exit 1
cd staging-cd || exit 1

# Patch the CSI controller patch yaml file with the driver and syncer images from build job.
yq -i '(.spec.template.spec.containers[0].image = env(VSPHERE_CSI_CONTROLLER_IMAGE)) | (.spec.template.spec.containers[1].image = env(VSPHERE_SYNCER_IMAGE))' staging/csi-controller-patch.yaml || exit 1

# Patch the CSI webhook patch yaml file with the syncer images from build job.
yq -i '(.spec.template.spec.containers[0].image = env(VSPHERE_SYNCER_IMAGE))' staging/csi-webhook-patch.yaml || exit 1

# The kubeconfig has restricted read-only access to only vmware-system-csi namespace in the Supervisor.
# It can only monitor the CSI deployments.
export KUBECONFIG=$CNS_STAGING_SV_KUBECONFIG
export K8S_MAJOR_VERSION
export K8S_MINOR_VERSION

K8S_MAJOR_VERSION=$(kubectl version -o json | jq .serverVersion.major | tr -d '"')
K8S_MINOR_VERSION=$(kubectl version -o json | jq .serverVersion.minor | tr -d '"')

echo "Supervisor version: $K8S_MAJOR_VERSION.$K8S_MINOR_VERSION"

# Replace the kubernetes version in kustomization.yaml
yq -i '.bases = ["../" + env(K8S_MAJOR_VERSION) + "." + env(K8S_MINOR_VERSION)]' staging/kustomization.yaml || exit 1

# If there are any changes, then commit the code changes and push it to the repo.
if git diff | grep diff;
then
	echo "Code changes to be pushed:"
	git diff
	git add . || exit 1
	git config user.email "svc.bot-cns@vmware.com" || exit 1
	git config user.name "svc.bot-cns" || exit 1
	git commit -m "Pipeline updated manifest files with images $VSPHERE_CSI_CONTROLLER_IMAGE and $VSPHERE_SYNCER_IMAGE" || exit 1
	git push origin main || exit 1
else
	echo "No code changes pushed to the staging repo."
fi

while true
do
	tmp_file=/tmp/$$
	kubectl -n vmware-system-csi get deployment vsphere-csi-controller -o=jsonpath="{'\n'}{range .spec.template.spec.containers[*]}{.image}{'\n'}{end}" | tee $tmp_file
	if grep "$VSPHERE_CSI_CONTROLLER_IMAGE" $tmp_file;
	then
		echo "CSI deployment is patched."
		break
	fi
	echo "CSI deployment not yet patched. Sleeping for 1 second..."
	sleep 1
done

echo "Sleeping for 1 min for WCP to reconcile the CSI patch.."
sleep 60

echo "Wait for 2 mins for the CSI deployment to be ready."
if ! kubectl wait deployment -n vmware-system-csi vsphere-csi-controller --for=jsonpath="{.status.readyReplicas}"=3 --timeout=120s;
then
	echo "CSI deployment is not ready within 120s."
	exit 1
fi
echo "Completed deploying CSI images to the staging environment."
