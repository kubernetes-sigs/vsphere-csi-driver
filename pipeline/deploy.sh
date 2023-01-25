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

# Borrow a testbed from CSI Testbed Pool Svc.
testbed=`curl -X 'GET' ${CNS_TESTBEDPOOL_SVC_URL}`
if [ $? -ne 0 ]
then
	echo "Unable to borrow a testbed!"
	exit 1
fi
echo "TestbedInfo: $testbed"

vcIp=`echo $testbed | jq '.vcIp'|tr -d '"'`
if [ $? -ne 0 ]
then
	echo "Error getting the vcIp!"
	exit 1
fi

echo "VC_IP=$vcIp" >> build.env

svAdminCreds=`echo $testbed | jq '.svAdminCreds'|tr -d '"'|base64 -d`
if [ $? -ne 0 ]
then
	echo "Error getting the svAdminCreds!"
	exit 1
fi
echo "svAdminCreds = $svAdminCreds"
SV_KUBECONFIG=/tmp/$$
echo $testbed | jq '.svAdminCreds'|tr -d '"'|base64 -d > $SV_KUBECONFIG
export KUBECONFIG=$SV_KUBECONFIG

kustomize build pipeline/dev | envsubst | kubectl apply -f -
if [ $? -ne 0 ]
then
	echo "Error patching the CSI images in the testbed!"
	exit 1
fi

# Sleep for 60 seconds so that k8s can act on the applied changes.
echo "Sleeping for 60 seconds..."
sleep 60

# Wait for 2 mins for the CSI deployment to be ready.
kubectl wait deployment -n vmware-system-csi vsphere-csi-controller --for=jsonpath="{.status.readyReplicas}"=3 --timeout=120s
if [ $? -ne 0 ]
then
	echo "CSI deployment is not ready within 120s."
	exit 1
fi

# Wait for 2 mins for the CSI webhook to be ready.
#kubectl wait deployment -n vmware-system-csi vsphere-csi-webhook --for=jsonpath="{.status.readyReplicas}"=3 --timeout=120s
#if [ $? -ne 0 ]
#then
#	echo "CSI webhook is not ready within 120s."
#	exit 1
#fi

echo "Successfully patched the CSI images."
