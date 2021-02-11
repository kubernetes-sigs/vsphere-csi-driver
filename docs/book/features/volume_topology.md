<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD024 -->
# vSphere CSI Driver - Volume Topology

- [Deploy workloads using topology with immediate volume binding mode](#deploy_workload_using_topology_immediate)
- [Deploy workloads using topology with WaitForFirstConsumer volume binding mode](#deploy_workload_using_topology_WaitForFirstConsumer)

**Note:** Volume Topology and Availability Zone feature is **beta** in vSphere CSI Driver.

Prerequisite : Enable topology in the Kubernetes Cluster. Follow steps mentioned in [Deployment with Topology](../driver-deployment/deploying_csi_with_zones.md).

## Deploy workloads using topology with `immediate` volume binding mode <a id="deploy_workload_using_topology_immediate"></a>

When topology is enabled in the cluster, you can deploy a Kubernetes workload to a specific region or zone defined in the topology.

Use the sample workflow to provision and verify your workloads.

### Procedure

1. Create a StorageClass that defines zone and region mapping.

   To the StorageClass YAML file, add zone-a and region-1 in the allowedTopologies field.

   ```bash
   tee example-zone-sc.yaml >/dev/null <<'EOF'
   kind: StorageClass
   apiVersion: storage.k8s.io/v1
   metadata:
     name: example-vanilla-block-zone-sc
   provisioner: csi.vsphere.vmware.com
   allowedTopologies:
     - matchLabelExpressions:
         - key: failure-domain.beta.kubernetes.io/zone
           values:
            - zone-a
         - key: failure-domain.beta.kubernetes.io/region
           values:
             - region-1
   EOF
   ```

   Note: Here `volumeBindingMode` will be `Immediate`, as it is default when not specified.

   ```bash
   kubectl create -f example-zone-sc.yaml
   storageclass.storage.k8s.io/example-vanilla-block-zone-sc created
   ```

2. Create a PersistenceVolumeClaim.

   ```bash
   tee example-zone-pvc.yaml >/dev/null <<'EOF'
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: example-vanilla-block-zone-pvc
   spec:
      accessModes:
      - ReadWriteOnce
      resources:
        requests:
          storage: 5Gi
      storageClassName: example-vanilla-block-zone-sc
    EOF
    ```

    ```bash
    kubectl create -f example-zone-pvc.yaml
    persistentvolumeclaim/example-vanilla-block-zone-pvc created
    ```

3. Verify that a volume is created for the PersistentVolumeClaim.

    ```bash
    kubectl get pvc example-vanilla-block-zone-pvc
    NAME                             STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                    AGE
    example-vanilla-block-zone-pvc   Bound    pvc-5b340a9b-a990-11e9-b26e-005056a04307   5Gi        RWO            example-vanilla-block-zone-sc   58s

    kubectl get pvc example-vanilla-block-zone-pvc
    NAME                             STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                    AGE
    example-vanilla-block-zone-pvc   Bound    pvc-5b340a9b-a990-11e9-b26e-005056a04307   5Gi        RWO            example-vanilla-block-zone-sc   91s
    ```

4. Verify that the persistent volume is provisioned with the `Node Affinity` rules containing zone and region specified in the StorageClass.

    ```bash
    kubectl describe pv pvc-5b340a9b-a990-11e9-b26e-005056a04307
    Name:              pvc-5b340a9b-a990-11e9-b26e-005056a04307
    Labels:            <none>
    Annotations:       pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com
    Finalizers:        [kubernetes.io/pv-protection]
    StorageClass:      example-vanilla-block-zone-sc
    Status:            Bound
    Claim:             default/example-vanilla-block-zone-pvc
    Reclaim Policy:    Delete
    Access Modes:      RWO
    VolumeMode:        Filesystem
    Capacity:          5Gi
    Node Affinity:
      Required Terms:
        Term 0:        failure-domain.beta.kubernetes.io/zone in [zone-a]
                       failure-domain.beta.kubernetes.io/region in [region-1]
    Message:  
    Source:
        Type:              CSI (a Container Storage Interface (CSI) volume source)
        Driver:            csi.vsphere.vmware.com
        VolumeHandle:      8f1f5e44-fafa-4404-91f7-d7a9bfd30e16
        ReadOnly:          false
        VolumeAttributes:      fstype=
                               storage.kubernetes.io/csiProvisionerIdentity=1563472725085-8081-csi.vsphere.vmware.com
                               type=vSphere CNS Block Volume
    Events:                <none>
    ```

5. Create a pod.

    ```bash
    tee example-zone-pod.yaml >/dev/null <<'EOF'
    apiVersion: v1
    kind: Pod
    metadata:
      name: example-vanilla-block-zone-pod
    spec:
      containers:
      - name: test-container
        image: gcr.io/google_containers/busybox:1.24
        command: ["/bin/sh", "-c", "echo
     'hello' > /mnt/volume1/index.html  && chmod o+rX /mnt
    /mnt/volume1/index.html && while true ; do sleep 2 ; done"]
        volumeMounts:
        - name: test-volume
          mountPath: /mnt/volume1
      restartPolicy: Never
      volumes:
      - name: test-volume
        persistentVolumeClaim:
          claimName: example-vanilla-block-zone-pvc
    EOF
    ```

    ```bash
    kubectl create -f example-zone-pod.yaml
    pod/example-vanilla-block-zone-pod created
    ```

    Pod is scheduled on the node k8s-node1 which belongs to zone: "zone-a" and region: "region-1"

    ```bash
    kubectl describe pod example-vanilla-block-zone-pod | egrep "Node:"
    Node:               k8s-node1/10.160.78.255
    ```

## Deploy workloads using topology with `WaitForFirstConsumer` volume binding mode <a id="deploy_workload_using_topology_WaitForFirstConsumer"></a>

The vSphere CSI driver supports topology-aware volume provisioning with `WaitForFirstConsumer`

Topology-aware provisioning allows Kubernetes to make intelligent decisions and find the best place to dynamically provision a volume for a pod. In multi-zone clusters, volumes are provisioned in an appropriate zone that can run your pod, allowing you to easily deploy and scale your stateful workloads across failure domains to provide high availability and fault tolerance.

`external-provisioner` must be deployed with the `--strict-topology` arguments.

This argument controls which topology information is passed to `CreateVolumeRequest.AccessibilityRequirements` in case of a delayed binding.

For information on how this option changes the result, see the table at [https://github.com/kubernetes-csi/external-provisioner#topology-support](https://github.com/kubernetes-csi/external-provisioner#topology-support). This option has no effect if the Topology feature is disabled or the Immediate volume binding mode is used.

### Procedure

1. Enable the Topology feature.

    Create a StorageClass with the `volumeBindingMode` parameter set to `WaitForFirstConsumer`.

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

    This new setting instructs the volume provisioner, instead of creating a volume immediately, to wait until a pod using an associated PVC runs through scheduling. Note that in the previous StorageClass, `failure-domain.beta.kubernetes.io/zone` and `failure-domain.beta.kubernetes.io/region` were specified in the allowedTopologies entry. You **do not** need to specify them again, as pod policies now drive the decision of which zone to use for a volume provisioning.

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
