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
Usage: Generate self-signed certificate for validation webhook service.
Patch validatingwebhook.yaml with CA_BUNDLE retrieved from self-signed certificate
and create ValidatingWebhookConfiguration and vsphere-webhook-svc service using patched yaml file

usage: ${0} [OPTIONS]
The following flags are required.
       --namespace        Namespace where webhook service and secret reside.
EOF
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case ${1} in
        --namespace)
            namespace="$2"
            shift
            ;;
        *)
            usage
            ;;
    esac
    shift
done

[ -z "${namespace}" ] && namespace=vmware-system-csi

service=vsphere-webhook-svc
secret=vsphere-webhook-certs


if [ ! -x "$(command -v openssl)" ]; then
    echo "openssl not found"
    exit 1
fi

if [ ! -x "$(command -v kubectl)" ]; then
    echo "kubectl not found"
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
  | openssl x509 -req -CA "${tmpdir}"/ca.crt -CAkey "${tmpdir}"/ca.key -days 180 -CAcreateserial -out "${tmpdir}"/webhook-server-tls.crt -extensions v3_req -extfile "${tmpdir}"/server.conf

cat <<eof >"${tmpdir}"/webhook.config
[WebHookConfig]
port = "8443"
cert-file = "/run/secrets/tls/tls.crt"
key-file = "/run/secrets/tls/tls.key"
eof

kubectl delete secret ${secret} --namespace "${namespace}" 2>/dev/null || true

# create the secret with CA cert and server cert/key
kubectl create secret generic "${secret}" \
        --from-file=tls.key="${tmpdir}"/webhook-server-tls.key \
        --from-file=tls.crt="${tmpdir}"/webhook-server-tls.crt \
        --from-file=webhook.config="${tmpdir}"/webhook.config \
        --dry-run=client -o yaml |
    kubectl -n "${namespace}" apply -f -


CA_BUNDLE="$(openssl base64 -A <"${tmpdir}/ca.crt")"
# clean-up previously created service and validatingwebhookconfiguration. Ignore errors if not present.

kubectl delete service vsphere-webhook-svc --namespace "${namespace}" 2>/dev/null || true
kubectl delete validatingwebhookconfiguration.admissionregistration.k8s.io validation.csi.vsphere.vmware.com --namespace "${namespace}" 2>/dev/null || true
kubectl delete serviceaccount vsphere-csi-webhook --namespace "${namespace}" 2>/dev/null || true
kubectl delete role.rbac.authorization.k8s.io vsphere-csi-webhook-role --namespace "${namespace}" 2>/dev/null || true
kubectl delete rolebinding.rbac.authorization.k8s.io vsphere-csi-webhook-role-binding --namespace "${namespace}" 2>/dev/null || true
kubectl delete clusterrole.rbac.authorization.k8s.io vsphere-csi-webhook-cluster-role 2>/dev/null || true
kubectl delete clusterrolebinding.rbac.authorization.k8s.io vsphere-csi-webhook-cluster-role-binding 2>/dev/null || true
kubectl delete deployment vsphere-csi-webhook --namespace "${namespace}" 2>/dev/null || true

# patch validatingwebhook.yaml with CA_BUNDLE and create service and validatingwebhookconfiguration
sed "s/caBundle: .*$/caBundle: ${CA_BUNDLE}/g" <validatingwebhook.yaml | kubectl apply -f -