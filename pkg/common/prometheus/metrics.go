/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// PrometheusBlockVolumeType represents the block volume type.
	PrometheusBlockVolumeType = "block"
	// PrometheusFileVolumeType represents the file volume type.
	PrometheusFileVolumeType = "file"
	// PrometheusUnknownVolumeType is used in situation when the volume type could not be found.
	PrometheusUnknownVolumeType = "unknown"

	// CSI operation types

	// PrometheusCreateVolumeOpType represents the CreateVolume operation.
	PrometheusCreateVolumeOpType = "create-volume"
	// PrometheusDeleteVolumeOpType represents the DeleteVolume operation.
	PrometheusDeleteVolumeOpType = "delete-volume"
	// PrometheusAttachVolumeOpType represents the AttachVolume operation.
	PrometheusAttachVolumeOpType = "attach-volume"
	// PrometheusDetachVolumeOpType represents the DetachVolume operation.
	PrometheusDetachVolumeOpType = "detach-volume"
	// PrometheusExpandVolumeOpType represents the ExpandVolume operation.
	PrometheusExpandVolumeOpType = "expand-volume"
	// PrometheusCreateSnapshotOpType represents CreateSnapshot operation.
	PrometheusCreateSnapshotOpType = "create-snapshot"
	// PrometheusDeleteSnapshotOpType represents DeleteSnapshot operation.
	PrometheusDeleteSnapshotOpType = "delete-snapshot"
	// PrometheusListSnapshotsOpType represents the ListSnapshots operation.
	PrometheusListSnapshotsOpType = "list-snapshot"
	// PrometheusListVolumeOpType represents the ListVolumes operation.
	PrometheusListVolumeOpType = "list-volume"

	// CNS operation types

	// PrometheusCnsCreateVolumeOpType represents the CreateVolume operation.
	PrometheusCnsCreateVolumeOpType = "create-volume"
	// PrometheusCnsDeleteVolumeOpType represents the DeleteVolume operation.
	PrometheusCnsDeleteVolumeOpType = "delete-volume"
	// PrometheusCnsAttachVolumeOpType represents the AttachVolume operation.
	PrometheusCnsAttachVolumeOpType = "attach-volume"
	// PrometheusCnsDetachVolumeOpType represents the DetachVolume operation.
	PrometheusCnsDetachVolumeOpType = "detach-volume"
	// PrometheusCnsUpdateVolumeMetadataOpType represents the UpdateVolumeMetadata operation.
	PrometheusCnsUpdateVolumeMetadataOpType = "update-volume-metadata"
	// PrometheusCnsUpdateVolumeCryptoOpType represents the UpdateVolumeCrypto operation.
	PrometheusCnsUpdateVolumeCryptoOpType = "update-volume-crypto"
	// PrometheusCnsExpandVolumeOpType represents the ExpandVolume operation.
	PrometheusCnsExpandVolumeOpType = "expand-volume"
	// PrometheusCnsQueryVolumeOpType represents the QueryVolume operation.
	PrometheusCnsQueryVolumeOpType = "query-volume"
	// PrometheusCnsQueryAllVolumeOpType represents the QueryAllVolume operation.
	PrometheusCnsQueryAllVolumeOpType = "query-all-volume"
	// PrometheusCnsQueryVolumeInfoOpType represents the QueryVolumeInfo operation.
	PrometheusCnsQueryVolumeInfoOpType = "query-volume-info"
	// PrometheusCnsRelocateVolumeOpType represents the RelocateVolume operation.
	PrometheusCnsRelocateVolumeOpType = "relocate-volume"
	// PrometheusCnsConfigureVolumeACLOpType represents the ConfigureVolumeAcl operation.
	PrometheusCnsConfigureVolumeACLOpType = "configure-volume-acl"
	// PrometheusQuerySnapshotsOpType represents QuerySnapshots operation.
	PrometheusQuerySnapshotsOpType = "query-snapshots"
	// PrometheusCnsCreateSnapshotOpType represents CreateSnapshot operation.
	PrometheusCnsCreateSnapshotOpType = "create-snapshot"
	// PrometheusCnsDeleteSnapshotOpType represents DeleteSnapshot operation.
	PrometheusCnsDeleteSnapshotOpType = "delete-snapshot"
	// PrometheusAccessibleVolumes represents accessible volumes.
	PrometheusAccessibleVolumes = "accessible-volumes"
	// PrometheusInaccessibleVolumes represents inaccessible volumes.
	PrometheusInaccessibleVolumes = "inaccessible-volumes"

	// PrometheusPassStatus represents a successful API run.
	PrometheusPassStatus = "pass"
	// PrometheusFailStatus represents an unsuccessful API run.
	PrometheusFailStatus = "fail"
)

var (
	// CsiInfo is a gauge metric to observe the CSI version.
	CsiInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vsphere_csi_info",
		Help: "CSI Info",
	}, []string{"version"})

	// SyncerInfo is a gauge metric to observe the CSI version.
	SyncerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vsphere_syncer_info",
		Help: "Syncer Info",
	}, []string{"version"})

	// CsiControlOpsHistVec is a histogram vector metric to observe various control
	// operations in CSI.
	CsiControlOpsHistVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "vsphere_csi_volume_ops_histogram",
		Help: "Histogram vector for CSI volume operations.",
		// Creating more buckets for operations that takes few seconds and less buckets
		// for those that are taking a long time. A CSI operation taking a long time is
		// unexpected and we don't have to be accurate(just approximation is fine).
		Buckets: []float64{2, 5, 10, 15, 20, 25, 30, 60, 120, 180},
	},
		// Possible voltype - "unknown", "block", "file"
		// Possible optype - "create-volume", "delete-volume", "attach-volume", "detach-volume", "expand-volume"
		// Possible status - "pass", "fail"
		[]string{"voltype", "optype", "status", "faulttype"})

	// CnsControlOpsHistVec is a histogram vector metric to observe various control
	// operations on CNS. Note that this captures the time taken by CNS into a bucket
	// as seen by the client(CSI in this case).
	CnsControlOpsHistVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "vsphere_cns_volume_ops_histogram",
		Help: "Histogram vector for CNS operations.",
		// Creating more buckets for operations that takes few seconds and less buckets
		// for those that are taking a long time. A CNS operation taking a long time is
		// unexpected and we don't have to be accurate(just approximation is fine).
		Buckets: []float64{2, 5, 10, 15, 20, 25, 30, 60, 120, 180},
	},
		// Possible optype - "create-volume", "delete-volume", "attach-volume", "detach-volume", "expand-volume", etc
		// Possible status - "pass", "fail"
		[]string{"optype", "status"})

	// VolumeHealthGaugeVec is a gauge metric to observe the number of accessible and inaccessible volumes.
	VolumeHealthGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vsphere_volume_health_gauge",
		Help: "Gauge for total number of accessible and inaccessible volumes",
	},
		// Possible volume_health_type - "accessible-volumes", "inaccessible-volumes"
		[]string{"volume_health_type"})

	// FullSyncOpsHistVec is a histogram vector metric to observe CSI Full Sync.
	FullSyncOpsHistVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "vsphere_full_sync_ops_histogram",
		Help: "Histogram vector for CSI Full Sync operations.",
		// Creating more buckets for operations that takes few seconds and less buckets
		// for those that are taking a long time. A Full Sync operation taking a long time is
		// unexpected and we don't have to be accurate(just approximation is fine).
		Buckets: []float64{2, 5, 10, 15, 20, 25, 30, 60, 120, 180},
	},
		// Possible status - "pass", "fail"
		[]string{"status"})

	RequestOpsMetric = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vsphere_request_ops_seconds",
		Help:    "Histogram vector for individual request to vCenter",
		Buckets: []float64{2, 5, 10, 15, 20, 25, 30, 60, 120, 180},
	}, []string{"request", "client", "status"})
)
