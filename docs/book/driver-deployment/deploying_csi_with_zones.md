<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD024 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Deployment with Topology

- [Set Up Zones in the vSphere CNS Environment](#set_up_zones_in_vsphere)
- [Enable Zones for the vSphere CSI Driver](#enable_zones_for_vsphere_csi)

**Note:** Volume Topology and Availability Zone feature is **beta** in vSphere CSI Driver.

When you deploy CSI in a vSphere environment that includes multiple data centers or host clusters, you can use zoning.

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

#### Enable Zones for the vSphere CSI Driver <a id="enable_zones_for_vsphere_csi"></a>

Install the vSphere CSI driver using the zone and region entries.

##### Procedure

1. In the [vsphere config secret](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/installation.html#create_csi_vsphereconf) file, add entries for region and zone.

   ```bash
   [Labels]
   region = k8s-region
   zone = k8s-zone
   ```

2. Make sure `external-provisioner` is deployed with the arguments `--feature-gates=Topology=true` and `--strict-topology`.
   - Uncomment lines in the yaml file marked with `needed only for topology aware setup`. - https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/v2.2.0/manifests/v2.2.0/deploy/vsphere-csi-controller-deployment.yaml#L160-L161

3. Make sure secret is mounted on all workload nodes as well. This is required to help node discover its topology.
   - Uncomment lines in the yaml file marked with `needed only for topology aware setup`.
      - https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/v2.2.0/manifests/v2.2.0/deploy/vsphere-csi-node-ds.yaml#L67-L68
      - https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/v2.2.0/manifests/v2.2.0/deploy/vsphere-csi-node-ds.yaml#L84-L86
      - https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/v2.2.0/manifests/v2.2.0/deploy/vsphere-csi-node-ds.yaml#L121-L123

4. After the installation, verify all `csinodes` objects has `topologyKeys`.

      ```bash
      kubectl get csinodes -o jsonpath='{range .items[*]}{.metadata.name} {.spec}{"\n"}{end}'
      k8s-node1 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node1 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
      k8s-node2 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node2 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
      k8s-node3 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node3 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
      k8s-node4 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node4 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
      k8s-node5 map[drivers:[map[name:csi.vsphere.vmware.com nodeID:k8s-node5 topologyKeys:[failure-domain.beta.kubernetes.io/region failure-domain.beta.kubernetes.io/zone]]]]
      ```

5. Verify labels `failure-domain.beta.kubernetes.io/region` and `failure-domain.beta.kubernetes.io/zone` should be applied to all nodes.

      ```bash
         kubectl get nodes -L failure-domain.beta.kubernetes.io/zone -L failure-domain.beta.kubernetes.io/region
         NAME         STATUS   ROLES    AGE   VERSION   ZONE     REGION
         k8s-master   Ready    master   32m   v1.19.0   zone-a   region-1
         k8s-node1    Ready    <none>   18m   v1.19.0   zone-a   region-1
         k8s-node2    Ready    <none>   18m   v1.19.0   zone-b   region-1
         k8s-node3    Ready    <none>   18m   v1.19.0   zone-b   region-1
         k8s-node4    Ready    <none>   18m   v1.19.0   zone-c   region-1
         k8s-node5    Ready    <none>   18m   v1.19.0   zone-c   region-1
      ```