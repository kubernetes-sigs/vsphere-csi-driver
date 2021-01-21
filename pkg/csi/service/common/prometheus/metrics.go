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

	// PrometheusCreateVolumeOpType represents the CreateVolume operation.
	PrometheusCreateVolumeOpType = "create-volume"
	// PrometheusDeleteVolumeOpType represents the DeleteVolume operation.
	PrometheusDeleteVolumeOpType = "delete-volume"

	// PrometheusPassStatus represents a successful API run.
	PrometheusPassStatus = "pass"
	// PrometheusFailStatus represents an unsuccessful API run.
	PrometheusFailStatus = "fail"
)

var (
	// CsiInfo is a gauge metric to observe the CSI version.
	CsiInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "csi_info",
		Help: "CSI Info",
	}, []string{"version"})

	// VolumeControlOpsHistVec is a histogram vector metric to observe various control
	// operations in CSI.
	VolumeControlOpsHistVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "csi_volume_ops_histogram",
		Help: "Histogram vector for CSI volume operations.",
		// Creating more buckets for operations that takes few seconds and less buckets
		// for those that are taking a long time. A CSI operation taking a long time is
		// unexpected and we don't have to be accurate(just approximation is fine).
		Buckets: []float64{1, 2, 3, 4, 5, 7, 10, 12, 15, 18, 20, 25, 30, 60, 120, 180, 300},
	},
		// Possible voltype - "unknown", "block", "file"
		// Possible optype - "create-volume", "delete-volume", "attach-volume", "detach-volume", "expand-volume"
		// Possible status - "pass", "fail"
		[]string{"voltype", "optype", "status"})
)
