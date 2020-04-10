<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD024 -->
# vSphere CSI Driver - Deployment with Topology

- [Set Up Zones in the vSphere CNS Environment](#set_up_zones_in_vsphere)
- [Enable Zones for the vSphere CPI and CSI Driver](#enable_zones_for_vsphere_cpi_and_csi)
- [Deploy Workloads Using Zones](#deploy_workload_using_zones)

When you deploy CPI and CSI in a vSphere environment that includes multiple data centers or host clusters, you can use zoning.

Zoning enables orchestration systems, like Kubernetes, to integrate with vSphere storage resources that are not equally available to all nodes. As a result, the orchestration system can make intelligent decisions when dynamically provisioning volumes, and avoid situations such as those where a pod cannot start because the storage resource it needs is not accessible.

## Set Up Zones in the vSphere CNS Environment <a id="set_up_zones_in_vsphere"></a>

Depending on your vSphere storage environment, you can use different deployment scenarios for zones. For example, you can have zones per host cluster, per data center, or have a combination of both.

In the following example, the vCenter Server environment includes three clusters with node VMs located on all three clusters.

![REGION_AND_ZONES](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/REGION_AND_ZONES.png)

The sample workflow creates zones per cluster and per data center.

### Procedure

- Create Zones Using vSphere Tags
  - You can use vSphere tags to label zones in your vSphere environment.
- Enable Zones for the CCM and CSI Driver
  - Install the CCM and the CSI driver using the zone and region entries.
  
#### Create Zones Using vSphere Tags

You can use vSphere tags to label zones in your vSphere environment.

The task assumes that your vCenter Server environment includes three clusters, cluster1, cluster2, and cluster3, with the node VMs on all three clusters. In the task, you create two tag categories, k8s-zone and k8s-region. You tag the clusters as three zones, zone-a, zone-b, and zone-c, and mark the data center as a region, region-1.

##### Prerequisites

Make sure that you have appropriate tagging privileges that control your ability to work with tags. See vSphere Tagging Privileges in the vSphere Security documentation.

**Note:** Ancestors of node VMs, such as host, cluster, and data center, must have the ReadOnly role set for the vSphere user configured to use the CSI driver and CCM. This is required to allow reading tags and categories to prepare nodes' topology.

##### Procedure

1. In the vSphere Client, create two tag categories, k8s-zone and k8s-region.

   For information, see [Create, Edit, or Delete a Tag Category](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vcenterhost.doc/GUID-BA3D1794-28F2-43F3-BCE9-3964CB207FB6.html) in the vCenter Server and Host Management documentation.

2. In each category, create appropriate zone tags.

   For information on creating tags, see Create, Edit, or Delete a Tag in the vCenter Server and Host Management documentation.

    <table>
    <thead>
    <tr>
    <th>Categories</th>
    <th>Tags</th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td>k8s-zone</td>
    <td>zone-a<br>zone-b<br>zone-c</td>
    </tr>
    <tr>
    <td>k8s-region</td>
    <td>region-1</td>
    </tr>
    </tbody>
    </table>

3. Apply corresponding tags to the data center and clusters as indicated in the table.

   For information, see Assign or Remove a Tag in the vCenter Server and Host Management documentation.

    <table>
    <thead>
    <tr>
    <th>vSphere Objects</th>
    <th>Tags</th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td>datacenter</td>
    <td>region-1</td>
    </tr>
    <tr>
    <td>cluster1</td>
    <td>zone-a</td>
    </tr>
    <tr>
    <td>cluster2</td>
    <td>zone-b</td>
    </tr>
    <tr>
    <td>cluster3</td>
    <td>zone-c</td>
    </tr>
    </tbody>
    </table>

#### Enable Zones for the vSphere CPI and CSI Driver <a id="enable_zones_for_vsphere_cpi_and_csi"></a>

Install the vSphere CPI and the CSI driver using the zone and region entries.

##### Procedure

1. Install the vSphere CPI.

   In the value fields of the configmap cloud-config file, specify region and zone.
   Make sure to add the names of categories you defined in vSphere, such as k8s-region and k8s-zone.

   ```bash
   [Global]
   insecure-flag = "true"

   [VirtualCenter "vCenter Server IP address"]
   user = "user"
   password = "password"
   port = "443"
   datacenters = "datacenter"

   [Network]
   public-network = "VM Network"

   [Labels]
   region = k8s-region
   zone = k8s-zone
   ```

   ```bash
   cd /etc/kubernetes
   kubectl create configmap cloud-config --from-file=vsphere.conf --namespace=kube-system

   kubectl apply -f https://raw.githubusercontent.com/kubernetes/cloud-provider-vsphere/master/manifests/controller-manager/cloud-controller-manager-roles.yaml
   kubectl apply -f https://raw.githubusercontent.com/kubernetes/cloud-provider-vsphere/master/manifests/controller-manager/cloud-controller-manager-role-bindings.yaml
   kubectl apply -f https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/controller-manager/vsphere-cloud-controller-manager-ds.yaml
   ```

2. Verify that your CCM installation is successful.

   After installation, labels `failure-domain.beta.kubernetes.io/region` and `failure-domain.beta.kubernetes.io/zone` are applied to all nodes.

   ```bash
   kubectl get nodes -L failure-domain.beta.kubernetes.io/zone -L failure-domain.beta.kubernetes.io/region
   NAME         STATUS   ROLES    AGE   VERSION   ZONE     REGION
   k8s-master   Ready    master   32m   v1.14.2   zone-a   region-1
   k8s-node1    Ready    <none>   18m   v1.14.2   zone-a   region-1
   k8s-node2    Ready    <none>   18m   v1.14.2   zone-b   region-1
   k8s-node3    Ready    <none>   18m   v1.14.2   zone-b   region-1
   k8s-node4    Ready    <none>   18m   v1.14.2   zone-c   region-1
   k8s-node5    Ready    <none>   18m   v1.14.2   zone-c   region-1
   ```

3. Install the CSI driver.

   Make sure `external-provisioner` is deployed with the arguments `--feature-gates=Topology=true`.

   In the credential secret file, add entries for region and zone.

   ```bash
   [Labels]
   region = k8s-region
   zone = k8s-zone
   ```

4. Verify that your CSI driver installation is successful.

   ```bash
   kubectl get csinodes -o jsonpath='{range .items[*]}{.metadata.name} {.spec}{"\n"}{end}'
   k8s-node1 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node1 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
   k8s-node2 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node2 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
   k8s-node3 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node3 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
   k8s-node4 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node4 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
   k8s-node5 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:
   ```

#### Deploy Workloads Using Zones <a id="deploy_workload_using_zones"></a>

With zones, you can deploy a Kubernetes workload to a specific region or zone.

Use the sample workflow to provision and verify your workloads.

##### Procedure

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

   ```bnf
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

4. Verify that the persistent volume is provisioned with the Node Affinity rules containing zone and region specified in the StorageClass.

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
