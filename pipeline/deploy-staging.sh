#!/bin/bash

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
cd staging-cd || exit 1

# Patch the yaml file with the driver and syncer images from build job.
yq -i '(.spec.template.spec.containers[0].image = env(VSPHERE_CSI_CONTROLLER_IMAGE)) | (.spec.template.spec.containers[1].image = env(VSPHERE_SYNCER_IMAGE))' staging/patch.yaml || exit 1

# If there are any changes, then commit the code changes and push it to the repo.
if git diff | grep diff;
then
	echo "Code changes to be pushed:"
	git diff
	git add . || exit 1
	git config user.email "svc.bot-cns@vmware.com" || exit 1
	git config user.name "svc.bot-cns" || exit 1
	git commit -m "Pipeline updated staging/patch.yaml with images $VSPHERE_CSI_CONTROLLER_IMAGE and $VSPHERE_SYNCER_IMAGE" || exit 1
	git push origin main || exit 1
else
	echo "No code changes pushed to the staging repo."
fi

# TODO: Add code to wait for the CD infra to update the CSI in the staging environment.

echo "Completed deploying CSI images to the staging environment."
