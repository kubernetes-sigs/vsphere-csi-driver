#!/bin/bash
# Copyright 2021 The Kubernetes Authors.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat <<EOF
Usage: Deploys the necessary components for CSI Snapshot feature for vSphere CSI driver.

Ensure that block-volume-snapshot feature is enabled.

1. Deploys the VolumeSnapshot CRDs
2. Creates RBAC rules to support VolumeSnapshot
3. Deploys snapshot-controller in kube-system namespace
4. Deploys the snapshot validation webhook
5. Patches vSphere CSI driver to deploy the csi-snapshotter sidecar

Refer to https://kubernetes-csi.github.io/docs/snapshot-controller.html for further information.

Example command:

./deploy-csi-snapshot-components.sh --release v5.0.1

usage: ${0} [OPTIONS]
The following flags are required.
       --release        The external-snapshot release files to use.
                        Default: v5.0.1
                        Qualified: v4.1.1, v5.0.0, v5.0.1
EOF
    exit 1
fi

if ! command -v kubectl > /dev/null; then
  echo "kubectl is missing"
  echo "Please refer to https://kubernetes.io/docs/tasks/tools/install-kubectl/ to install kubectl"
  exit 1
fi

feature_state=$(kubectl get configmap internal-feature-states.csi.vsphere.vmware.com -n vmware-system-csi -o jsonpath='{.data.block-volume-snapshot}')
if [ "$feature_state" = "true" ]
then
        echo -e "✅ Verified that block-volume-snapshot feature is enabled"
else
        echo -e "❌ ERROR: Please enable the block-volume-snapshot feature to proceed"
        exit 1
fi

while [[ $# -gt 0 ]]; do
    case ${1} in
        --release)
            release="$2"
            shift
            ;;
        *)
            usage
            ;;
    esac
    shift
done

qualified_releases=("v4.1.1" "v5.0.0" "v5.0.1")

if [ -z "${release}" ]
then
  release=v5.0.1
else
  if [[ ! "${qualified_releases[*]}" =~ ${release} ]]
  then
    echo -e "❗ WARNING: Attempting to use ${release} version, only v4.1.1, v5.0.0 and v5.0.1 are qualified versions"
  fi
fi
echo "Using release version: ${release}"

# Waits for deployment to complete. $1: name of deployment, $2: namespace.
wait_for_deployment() {
local deployed=false
local requiredReplicas
local availableReplicas
for _ in $(seq 20); do
    requiredReplicas=$(kubectl get deployment "$1" -n "$2" -o jsonpath='{.spec.replicas}')
    availableReplicas=$(kubectl get deployment "$1" -n "$2" -o jsonpath='{.status.availableReplicas}')
    if [[ ${availableReplicas} == "${requiredReplicas}" ]]; then
        deployed=true
        break
    fi
    echo "waiting for $1 to complete.."
    sleep 10
done

if [ $deployed ]
then
  echo -e "✅ $1 successfully deployed!"
else
  echo -e "❌ ERROR: Failed to deploy $1"
  exit 1
fi
}

# Deploy the snapshot controller.
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${release}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml 2>/dev/null || true
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${release}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml 2>/dev/null || true
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${release}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml 2>/dev/null || true
echo  -e "✅ Deployed VolumeSnapshot CRDs"
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${release}"/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml 2>/dev/null || true
echo -e "✅ Created  RBACs for snapshot-controller"
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${release}"/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml 2>/dev/null || true
echo -e "✅ Deployed snapshot-controller"

kubectl patch deployment -n kube-system snapshot-controller --patch '{"spec": {"template": {"spec": {"nodeSelector": {"node-role.kubernetes.io/master": ""}, "tolerations": [{"key":"node-role.kubernetes.io/master","operator":"Exists", "effect":"NoSchedule"}]}}}}'

kubectl patch deployment -n kube-system snapshot-controller \
--type=json \
-p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kube-api-qps=100"},{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kube-api-burst=100"}]'

wait_for_deployment snapshot-controller kube-system

# Deploy the snapshot validating webhook.
service=snapshot-validation-service
secret=snapshot-webhook-certs
namespace=kube-system

if [ ! -x "$(command -v openssl)" ]; then
    echo "openssl not found"
    exit 1
fi

tmpdir=$(mktemp -d)
echo "creating certs in tmpdir ${tmpdir} "

cat <<EOF >> "${tmpdir}"/server.conf
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
prompt = no
[req_distinguished_name]
CN = ${service}.${namespace}.svc
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${service}
DNS.2 = ${service}.${namespace}
DNS.3 = ${service}.${namespace}.svc
EOF

# Generate the CA cert and private key
openssl req -nodes -new -x509 -keyout "${tmpdir}"/ca.key -out "${tmpdir}"/ca.crt -subj "/CN=vSphere CSI Admission Controller Webhook CA"
openssl genrsa -out "${tmpdir}"/webhook-server-tls.key 2048
openssl req -new -key "${tmpdir}"/webhook-server-tls.key -subj "/CN=${service}.${namespace}.svc" -config "${tmpdir}"/server.conf \
  | openssl x509 -req -CA "${tmpdir}"/ca.crt -CAkey "${tmpdir}"/ca.key -CAcreateserial -out "${tmpdir}"/webhook-server-tls.crt -extensions v3_req -extfile "${tmpdir}"/server.conf

cat <<eof >"${tmpdir}"/webhook.config
[WebHookConfig]
port = "8443"
cert-file = "/run/secrets/tls/tls.crt"
key-file = "/run/secrets/tls/tls.key"
eof

# Cleanup previous secret if exists.
kubectl delete secret ${secret} --namespace "${namespace}" 2>/dev/null || true
# create the secret with CA cert and server cert/key
kubectl create secret generic "${secret}" \
        --from-file=tls.key="${tmpdir}"/webhook-server-tls.key \
        --from-file=tls.crt="${tmpdir}"/webhook-server-tls.crt \
        --from-file=webhook.config="${tmpdir}"/webhook.config \
        --dry-run=client -o yaml |
    kubectl -n "${namespace}" apply -f -

CA_BUNDLE="$(openssl base64 -A <"${tmpdir}/ca.crt")"

# clean-up previously created service and validatingwebhookconfiguration.
kubectl delete service "${service}" --namespace "${namespace}" 2>/dev/null || true
kubectl delete validation-webhook.snapshot.storage.k8s.io --namespace "${namespace}" 2>/dev/null || true
kubectl delete deployment snapshot-validation-deployment --namespace "${namespace}" 2>/dev/null || true

# patch csi-snapshot-validatingwebhook.yaml with CA_BUNDLE and create service and validatingwebhookconfiguration
curl https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/manifests/vanilla/csi-snapshot-validatingwebhook.yaml | sed "s/caBundle: .*$/caBundle: ${CA_BUNDLE}/g" | kubectl apply -f -
echo -e "✅ Deployed snapshot-validation-deployment"

kubectl patch deployment -n kube-system snapshot-validation-deployment --patch '{"spec": {"template": {"spec": {"nodeSelector": {"node-role.kubernetes.io/master": ""}, "tolerations": [{"key":"node-role.kubernetes.io/master","operator":"Exists", "effect":"NoSchedule"}]}}}}'

wait_for_deployment snapshot-validation-deployment kube-system

# Update the vSphere CSI driver to add the snapshot side car.
tmpdir=$(mktemp -d)
echo "creating patch file in tmpdir ${tmpdir}"
cat <<EOF >> "${tmpdir}"/patch.yaml
spec:
  template:
    spec:
      containers:
        - name: csi-snapshotter
          image: 'k8s.gcr.io/sig-storage/csi-snapshotter:${release}'
          args:
            - '--v=4'
            - '--kube-api-qps=100'
            - '--kube-api-burst=100'
            - '--timeout=300s'
            - '--csi-address=\$(ADDRESS)'
            - '--leader-election'
          env:
            - name: ADDRESS
              value: /csi/csi.sock
          volumeMounts:
            - mountPath: /csi
              name: socket-dir
EOF

numOfCSIDriverRequiredReplicas=$(kubectl get deployment vsphere-csi-controller -n vmware-system-csi -o jsonpath='{.spec.replicas}')
numOfCSIDriverAvailableReplicas=$(kubectl get deployment vsphere-csi-controller -n vmware-system-csi -o jsonpath='{.status.availableReplicas}')

echo -e "Scale down the vSphere CSI driver"
kubectl scale deployment vsphere-csi-controller -n vmware-system-csi --replicas=0

echo -e "Patching vSphere CSI driver.."
kubectl patch deployment vsphere-csi-controller -n vmware-system-csi --patch "$(cat "${tmpdir}"/patch.yaml)"

echo -e "Scaling the vSphere CSI driver back to original state.."
kubectl scale deployment vsphere-csi-controller -n vmware-system-csi --replicas="${numOfCSIDriverRequiredReplicas}"
echo -e "\n"
# Wait till at least the original number of replicas are available
for _ in $(seq 20); do
    currentReplicas=$(kubectl get deployment vsphere-csi-controller -n vmware-system-csi -o jsonpath='{.status.availableReplicas}')
    if [[ ${currentReplicas} == "" ]]; then
            currentReplicas=0
    fi
    if [[ ${currentReplicas} -ge "${numOfCSIDriverAvailableReplicas}" ]]; then
        echo -e "vSphere CSI driver restored to original number of replicas before patching!\n"
        break
    fi
    echo "waiting for vsphere-csi-controller deployment to scale.."
    sleep 10
done

echo -e "\n✅ Successfully deployed all components for CSI Snapshot feature, please wait till vSphere CSI driver deployment is updated..\n"
