#!/bin/bash
# Copyright 2022 The Kubernetes Authors.

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

The script fails if there is existing snapshot-controller or snapshot validation webhook with unqualified versions
Deleting the unqualified snapshot-controller or snapshot validation webhook running the script again deploys the qualified version

The script fails if incorrect version VolumeSnapshot CRDs exists. Deleting the CRDs will deploy the correct version
of the CRDs.

The script patches the vSphere CSI driver with the qualified csi-snapshotter sidecar version.

Refer to https://kubernetes-csi.github.io/docs/snapshot-controller.html for further information.

Example command:

./deploy-csi-snapshot-components.sh
EOF
    exit 0
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

qualified_version="v6.2.2"
volumesnapshotclasses_crd="volumesnapshotclasses.snapshot.storage.k8s.io"
volumesnapshotcontents_crd="volumesnapshotcontents.snapshot.storage.k8s.io"
volumesnapshots_crd="volumesnapshots.snapshot.storage.k8s.io"

is_deployment_available(){
	if ! output=$(kubectl get deployment "$1" -n "$2" 2>&1); then
		echo "false"
	else
		echo "true"
	fi
}

validate_version(){
	local container_image
	local deployment_name
	local container_image_version
	deployment_name=$1
	container_image=$(kubectl get deployment "$1" -n "$2" -o jsonpath='{.spec.template.spec.containers[0].image}')
	echo "${deployment_name} image : ${container_image}"
	# Set comma as delimiter
	IFS=':'
	read -r -a strarr <<< "$container_image"
	container_image_version=${strarr[1]}
	echo "${deployment_name} version : ${container_image_version}"
	# shellcheck disable=SC2154
	if [ "$container_image_version" = "$qualified_version" ]
	then
		echo -e "✅ Verified that running ${deployment_name} is using the qualified version ${qualified_version}"
	else
		echo -e "❌ ERROR: ${container_image_version} for ${deployment_name} is not qualified for vSphere CSI Driver, only ${qualified_version} is supported"
		exit 1
	fi
}

is_crd_available(){
	# shellcheck disable=SC2034
	if ! output=$(kubectl get crd "$1" 2>&1); then
		echo "false"
	else
		echo "true"
	fi
}

check_crd_version(){
  local crd=$1
  local valid=false
  crd_ver_info=$(kubectl get crd "$1" -o jsonpath='{.spec.versions[*].name}')
  # Set comma as delimiter
  IFS=' '
  read -r -a crd_versions <<< "$crd_ver_info"
  for ver in "${crd_versions[@]}"; do
  		if [ "$ver" = "v1" ]
  		then
  			valid="true"
  		fi
  done
  if [ "$valid" = "false" ]
  then
    echo -e "❌ ERROR: Unsupported versions [$crd_ver_info] present for CRD $crd, please explicitly upgrade crd to v1 before re-running the script.."
    exit 1
  else
    echo -e "Supported version v1 of crd $crd present"
  fi
}

check_and_deploy_crds(){
	echo -e "Checking CRDs..."
	volumesnapshotclasses_crd_available=$(is_crd_available $volumesnapshotclasses_crd)
	if [ "$volumesnapshotclasses_crd_available" = "true" ]
	then
		check_crd_version ${volumesnapshotclasses_crd}
	else
		kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${qualified_version}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml 2>/dev/null || true
		echo -e "✅ Created CRD ${volumesnapshotclasses_crd}"
	fi

	volumesnapshotcontents_crd_available=$(is_crd_available $volumesnapshotcontents_crd)
	if [ "$volumesnapshotcontents_crd_available" = "true" ]
	then
		check_crd_version ${volumesnapshotcontents_crd}
	else
		kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${qualified_version}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml 2>/dev/null || true
		echo -e "✅ Created CRD ${volumesnapshotcontents_crd}"
	fi

	volumesnapshots_crd_available=$(is_crd_available $volumesnapshots_crd)
	if [ "$volumesnapshots_crd_available" = "true" ]
	then
		check_crd_version ${volumesnapshots_crd}
	else
		kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${qualified_version}"/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml 2>/dev/null || true
		echo -e "✅ Created CRD ${volumesnapshots_crd}"
	fi
	echo  -e "\n✅ Deployed VolumeSnapshot CRDs\n"
}

deploy_snapshot_controller(){
	echo -e "Start snapshot-controller deployment..."
	kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${qualified_version}"/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml 2>/dev/null || true
	echo -e "✅ Created  RBACs for snapshot-controller"
	kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/"${qualified_version}"/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml 2>/dev/null || true
	kubectl -n kube-system set image deployment/snapshot-controller snapshot-controller=registry.k8s.io/sig-storage/snapshot-controller:"${qualified_version}"
	kubectl patch deployment -n kube-system snapshot-controller --patch '{"spec": {"template": {"spec": {"nodeSelector": {"node-role.kubernetes.io/control-plane": ""}, "tolerations": [{"key":"node-role.kubernetes.io/master","operator":"Exists", "effect":"NoSchedule"},{"key":"node-role.kubernetes.io/control-plane","operator":"Exists", "effect":"NoSchedule"}]}}}}'
	kubectl patch deployment -n kube-system snapshot-controller --type=json \
	-p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kube-api-qps=100"},{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kube-api-burst=100"}]'
	kubectl -n kube-system rollout status deploy/snapshot-controller
	echo -e "\n✅ Successfully deployed snapshot-controller\n"
}

deploy_validation_webhook() {
	service=snapshot-validation-service
	secret=snapshot-webhook-certs
	namespace=kube-system
	if [ ! -x "$(command -v openssl)" ]; then
		echo "❌ ERROR: openssl not found"
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

	openssl req -nodes -new -x509 -keyout "${tmpdir}"/ca.key -out "${tmpdir}"/ca.crt -subj "/CN=vSphere CSI Admission Controller Webhook CA"
	openssl genrsa -out "${tmpdir}"/webhook-server-tls.key 2048
	openssl req -new -key "${tmpdir}"/webhook-server-tls.key -subj "/CN=${service}.${namespace}.svc" -config "${tmpdir}"/server.conf \
	| openssl x509 -req -CA "${tmpdir}"/ca.crt -CAkey "${tmpdir}"/ca.key -days 180 -CAcreateserial -out "${tmpdir}"/webhook-server-tls.crt -extensions v3_req -extfile "${tmpdir}"/server.conf
	cat <<EOF >"${tmpdir}"/webhook.config
	[WebHookConfig]
	port = "8443"
	cert-file = "/run/secrets/tls/tls.crt"
	key-file = "/run/secrets/tls/tls.key"
EOF
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
	kubectl patch deployment -n kube-system snapshot-validation-deployment --patch '{"spec": {"template": {"spec": {"nodeSelector": {"node-role.kubernetes.io/control-plane": ""}, "tolerations": [{"key":"node-role.kubernetes.io/master","operator":"Exists", "effect":"NoSchedule"},{"key":"node-role.kubernetes.io/control-plane","operator":"Exists", "effect":"NoSchedule"}]}}}}'
	kubectl -n kube-system rollout status deploy/snapshot-validation-deployment
	echo -e "\n✅ Successfully deployed snapshot-validation-deployment\n"
}

patch_vsphere_csi_driver(){
	tmpdir=$(mktemp -d)
	echo "creating patch file in tmpdir ${tmpdir}"
	cat <<EOF >> "${tmpdir}"/patch.yaml
spec:
  template:
    spec:
      containers:
        - name: csi-snapshotter
          image: 'registry.k8s.io/sig-storage/csi-snapshotter:${qualified_version}'
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
	echo -e "Scale down the vSphere CSI driver"
	kubectl scale deployment vsphere-csi-controller -n vmware-system-csi --replicas=0
	echo -e "Patching vSphere CSI driver.."
	kubectl patch deployment vsphere-csi-controller -n vmware-system-csi --patch "$(cat "${tmpdir}"/patch.yaml)"
	echo -e "Scaling the vSphere CSI driver back to original state.."
	kubectl scale deployment vsphere-csi-controller -n vmware-system-csi --replicas="${numOfCSIDriverRequiredReplicas}"
	kubectl -n vmware-system-csi rollout status deploy/vsphere-csi-controller
}

check_snapshotter_sidecar(){
	local found="false"
	local container_images
	local csi_snapshotter_image="registry.k8s.io/sig-storage/csi-snapshotter"
	container_images=$(kubectl -n vmware-system-csi get deployment vsphere-csi-controller -o jsonpath='{.spec.template.spec.containers[*].image}')
	IFS=' '
	read -r -a container_images_arr <<< "$container_images"
	for image in "${container_images_arr[@]}"; do
		local strarr
		IFS=':'
		read -r -a strarr <<< "$image"
		conatiner_image_name=${strarr[0]}
		container_image_version=${strarr[1]}
		if [ "$conatiner_image_name" = "$csi_snapshotter_image" ]
		then
			found="true"
			if [ "$container_image_version" = "$qualified_version" ]
			then
				echo -e "✅ vSphere CSI Driver already running the qualified version of csi-snapshotter."
				echo -e "\n✅ Successfully deployed all components for CSI Snapshot feature! \n"
				exit 0
			else
				echo -e "The running csi-snapshotter is not running the qualified version ${qualified_version}, patching deployment"
				patch_vsphere_csi_driver
				echo -e "\n✅ Successfully deployed all components for CSI Snapshot feature!\n"
			fi
		fi
	done
	if [ "$found" = "false" ]
	then
		echo -e "csi-snapshotter side-car not found in vSphere CSI Driver Deployment, patching.."
		patch_vsphere_csi_driver
		echo -e "\n✅ Successfully deployed all components for CSI Snapshot feature!\n"
	fi
}

# Check if CRDs exist, if they do, then validate the version, if not then deploy them
check_and_deploy_crds

snap_controller_available=$(is_deployment_available snapshot-controller kube-system)
if [ "$snap_controller_available" = "true" ]
then
  echo -e "snapshot-controller Deployment already exists, verifying version.."
	validate_version snapshot-controller kube-system
else
  echo -e "No existing snapshot-controller Deployment found, deploying it now.."
  deploy_snapshot_controller
fi

snap_validation_webhook_available=$(is_deployment_available snapshot-validation-deployment kube-system)
if [ "$snap_validation_webhook_available" = "true" ]
then
	echo -e "snapshot-validation-deployment Deployment already exists, verifying version.."
	validate_version snapshot-validation-deployment kube-system
else
  echo -e "No existing snapshot-validation-deployment Deployment found, deploying it now.."
  deploy_validation_webhook
fi

# Check if vSphere CSI Driver has the snapshotter sidecar, if not patch the deployment
check_snapshotter_sidecar