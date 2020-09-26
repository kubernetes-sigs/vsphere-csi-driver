#!/bin/bash
# Copyright 2020 The Kubernetes Authors.

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
Usage: Patch validatingwebhook.yaml with CA_BUNDLE retrieved from Kubernetes API server
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

[ -z "${namespace}" ] && namespace=kube-system

CA_BUNDLE=$(kubectl get configmap -n kube-system extension-apiserver-authentication -o=jsonpath='{.data.client-ca-file}' | base64 | tr -d '\n')

# clean-up previously created service and validatingwebhookconfiguration. Ignore errors if not present.

kubectl delete service vsphere-webhook-svc --namespace "${namespace}" 2>/dev/null || true
kubectl delete validatingwebhookconfiguration.admissionregistration.k8s.io validation.csi.vsphere.vmware.com --namespace "${namespace}" 2>/dev/null || true
kubectl delete serviceaccount vsphere-csi-webhook --namespace "${namespace}" 2>/dev/null || true
kubectl delete clusterrole.rbac.authorization.k8s.io vsphere-csi-webhook-role 2>/dev/null || true
kubectl delete clusterrolebinding.rbac.authorization.k8s.io vsphere-csi-webhook-role-binding --namespace "${namespace}" 2>/dev/null || true
kubectl delete deployment vsphere-csi-webhook --namespace "${namespace}" || true

# patch validatingwebhook.yaml with CA_BUNDLE and create service and validatingwebhookconfiguration
sed "s/caBundle: .*$/caBundle: ${CA_BUNDLE}/g" validatingwebhook.yaml | kubectl apply -f -
