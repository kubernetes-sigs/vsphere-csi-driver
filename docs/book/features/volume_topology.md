<!-- markdownlint-disable MD033 -->
# vSphere CSI Driver - Volume Topology

The vSphere CSI driver supports topology-aware volume provisioning. Topology-aware provisioning allows Kubernetes to make intelligent decisions and find the best place to dynamically provision a volume for a pod. In multi-zone clusters, volumes are provisioned in an appropriate zone that can run your pod, allowing you to easily deploy and scale your stateful workloads across failure domains to provide high availability and fault tolerance.

## Prerequisites

`external-provisioner` must be deployed with the `--strict-topology` arguments.

This argument controls which topology information is passed to `CreateVolumeRequest.AccessibilityRequirements` in case of a delayed binding.

For information on how this option changes the result, see the table at [https://github.com/kubernetes-csi/external-provisioner#topology-support](https://github.com/kubernetes-csi/external-provisioner#topology-support). This option has no effect if the Topology feature is disabled or the Immediate volume binding mode is used.

## Procedure

1. Enable the Topology feature.

    Create a StorageClass with the volumeBindingMode parameter set to WaitForFirstConsumer.

    ```bash
    tee topology-aware-standard.yaml >/dev/null <<'EOF'
    apiVersion: v1
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: topology-aware-standard
    provisioner: csi.vsphere.vmware.com
    volumeBindingMode: WaitForFirstConsumer
    EOF
    ```

    ```bash
    kubectl create -f topology-aware-standard.yaml
    storageclass.storage.k8s.io/topology-aware-standard created
    ```

    This new setting instructs the volume provisioner, instead of creating a volume immediately, to wait until a pod using an associated PVC runs through scheduling. Note that in the previous StorageClass, `failure-domain.beta.kubernetes.io/zone` and `failure-domain.beta.kubernetes.io/region` were specified in the allowedTopologies entry. You do not need to specify them again, as pod policies now drive the decision of which zone to use for a volume provisioning.

2. Create a pod and PVC using the StorageClass created previously.

    The following example demonstrates multiple pod constraints and scheduling policies.

    ```bash
    tee topology-aware-statefulset.yaml >/dev/null <<'EOF'
    ---
    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: web
    spec:
      replicas: 2
      selector:
        matchLabels:
          app: nginx
      serviceName: nginx
      template:
        metadata:
          labels:
            app: nginx
        spec:
          affinity:
            nodeAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                nodeSelectorTerms:
                  -
                    matchExpressions:
                      -
                        key: failure-domain.beta.kubernetes.io/zone
                        operator: In
                        values:
                          - zone-a
                          - zone-b
            podAntiAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                -
                  labelSelector:
                    matchExpressions:
                      -
                        key: app
                        operator: In
                        values:
                          - nginx
                  topologyKey: failure-domain.beta.kubernetes.io/zone
          containers:
            - name: nginx
              image: gcr.io/google_containers/nginx-slim:0.8
              ports:
                - containerPort: 80
                  name: web
              volumeMounts:
                - name: www
                  mountPath: /usr/share/nginx/html
                - name: logs
                  mountPath: /logs
      volumeClaimTemplates:
        - metadata:
            name: www
          spec:
            accessModes: [ "ReadWriteOnce" ]
            storageClassName: topology-aware-standard
            resources:
              requests:
                storage: 5Gi
        - metadata:
            name: logs
          spec:
            accessModes: [ "ReadWriteOnce" ]
            storageClassName: topology-aware-standard
            resources:
              requests:
                storage: 1Gi
    EOF
    ```

    ```bash
    kubectl create -f topology-aware-statefulset.yaml
    statefulset.apps/web created
    ```

3. Verify statefulset is up and running.

    ```bash
    kubectl get statefulset
    NAME   READY   AGE
    web    2/2     5m1s
    ```

4. Review your pods and your nods.

    Pods are created in the `zone-a` and `zone-b` specified in the nodeAffinity entry. `web-0` is scheduled on the node `k8s-node3`, which belongs to `zone-b`. `web-1` is scheduled on the node `k8s-node1`, which belongs to `zone-a`.

    ```bash
    kubectl get pods -o json | egrep "hostname|nodeName|claimName"
                    "hostname": "web-0",
                    "nodeName": "k8s-node3",
                                "claimName": "www-web-0"
                                "claimName": "logs-web-0"
                    "hostname": "web-1",
                    "nodeName": "k8s-node1",
                                "claimName": "www-web-1"
                                "claimName": "logs-web-1"
    ```

    ```bash
    kubectl get nodes k8s-node3 k8s-node1 -L failure-domain.beta.kubernetes.io/zone -L failure-domain.beta.kubernetes.io/region --no-headers
    k8s-node3   Ready   <none>   25h   v1.14.2   zone-b   region-1
    k8s-node1   Ready   <none>   25h   v1.14.2   zone-a   region-1
    ```

5. Verify volumes are provisioned in zones according to the policies set by the pod.

    ```bash
    $ kubectl describe pvc www-web-0 | egrep "volume.kubernetes.io/selected-node"
                   volume.kubernetes.io/selected-node: k8s-node3

    $ kubectl describe pvc logs-web-0 | egrep "volume.kubernetes.io/selected-node"
                   volume.kubernetes.io/selected-node: k8s-node3

    $ kubectl describe pvc www-web-1 | egrep "volume.kubernetes.io/selected-node"
                   volume.kubernetes.io/selected-node: k8s-node1

    $ kubectl describe pvc logs-web-1 | egrep "volume.kubernetes.io/selected-node"
                   volume.kubernetes.io/selected-node: k8s-node1
    ```

    ```bash
    $ kubectl get pv -o=jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.claimRef.name}{"\t"}{.spec.nodeAffinity}{"\n"}{end}'
    pvc-2253dc52-a9ed-11e9-b26e-005056a04307    www-web-0    map[required:map[nodeSelectorTerms:[map[matchExpressions:[map[key:failure-domain.beta.kubernetes.io/region operator:In values:[region-1]] map[operator:In values:[zone-b] key:failure-domain.beta.kubernetes.io/zone]]]]]]
    pvc-22575240-a9ed-11e9-b26e-005056a04307    logs-web-0    map[required:map[nodeSelectorTerms:[map[matchExpressions:[map[key:failure-domain.beta.kubernetes.io/zone operator:In values:[zone-b]] map[key:failure-domain.beta.kubernetes.io/region operator:In values:[region-1]]]]]]]
    pvc-3c963150-a9ed-11e9-b26e-005056a04307    www-web-1    map[required:map[nodeSelectorTerms:[map[matchExpressions:[map[key:failure-domain.beta.kubernetes.io/zone operator:In values:[zone-a]] map[operator:In values:[region-1] key:failure-domain.beta.kubernetes.io/region]]]]]]
    pvc-3c98978f-a9ed-11e9-b26e-005056a04307    logs-web-1    map[required:map[nodeSelectorTerms:[map[matchExpressions:[map[key:failure-domain.beta.kubernetes.io/zone operator:In values:[zone-a]] map[key:failure-domain.beta.kubernetes.io/region operator:In values:[region-1]]]]]]]
    ```

6. If required, specify allowedTopologies.

    When a cluster operator specifies the WaitForFirstConsumer volume binding mode, it is no longer necessary to restrict provisioning to specific topologies in most situations. However, if required, you can specify allowedTopologies.

    The following example demonstrates how to restrict the topology to specific zone.

    ```bash
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: example-vanilla-block-sc
    provisioner: csi.vsphere.vmware.com
    volumeBindingMode: WaitForFirstConsumer
    allowedTopologies:
      - matchLabelExpressions:
          - key: failure-domain.beta.kubernetes.io/zone
            values:
              - zone-b
          - key: failure-domain.beta.kubernetes.io/region
            values:
              - region-1
    ```
