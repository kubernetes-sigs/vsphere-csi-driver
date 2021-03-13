<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->

# vSphere CSI Driver - Cluster Distribution

- [Introduction](#introduction)
- [Deploy vSphere CSI driver with cluster-distribution](#vsphere-conf-cluster-distribution)
- [Limitations](#limitations)

**Note:** Feature to support `cluster-distribution` parameter in vSphere CSI driver is not part of any release yet.

## Introduction <a id="introduction"></a>

A new parameter, called `cluster-distribution`, is introduced to the `csi-vsphere.conf` file in an effort to collect telemetry data for analytics on vSphere Cloud Native Storage (CNS).

This optional parameter needs to be set by the Kubernetes distribution.

In future releases, this parameter will be made mandatory.

## Deploy vSphere CSI driver with cluster-distribution<a id="vsphere-conf-cluster-distribution"></a>

`cluster-distribution` field is added to the `Global` section of the `csi-vsphere.conf` file. An example `csi-vsphere.conf` file would look like:

```cgo
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
cluster-id = "<cluster-id>"
cluster-distribution = "<cluster-distribution>" # optional parameter. Examples are "Openshift", "Anthos", "PKS".

[VirtualCenter "<IP or FQDN>"]
insecure-flag = "<true or false>"
user = "<username>"
password = "<password>"
port = "<port>"
datacenters = "<datacenter1-path>, <datacenter2-path>, ..."
```

## Limitations <a id="limitations"></a>

- `cluster-distribution` values with a special character `\r` causes the vSphere CSI controller to go into CrashLoopBackOff state.

- `cluster-distribution` values of more than 128 characters will cause the PVC creation to be stuck in `Pending` state.
