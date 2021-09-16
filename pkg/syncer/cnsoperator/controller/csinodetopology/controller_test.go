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

package csinodetopology

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
)

func TestFindExistingTopologyLabels(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                string
		nodeList            []corev1.Node
		expectedZoneLabel   string
		expectedRegionLabel string
		expectedErr         bool
	}{
		{
			name: "ExpectedGALabels",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelTopologyZone:   "zone1",
							corev1.LabelTopologyRegion: "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelTopologyZone:   "zone2",
							corev1.LabelTopologyRegion: "regionB",
						},
					},
				},
			},
			expectedZoneLabel:   corev1.LabelTopologyZone,
			expectedRegionLabel: corev1.LabelTopologyRegion,
			expectedErr:         false,
		},
		{
			name: "ExpectedPartialGALabels",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							common.TopologyLabelsDomain + "/zone":   "zone1",
							common.TopologyLabelsDomain + "/region": "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelTopologyZone: "zone2",
						},
					},
				},
			},
			expectedZoneLabel:   corev1.LabelTopologyZone,
			expectedRegionLabel: corev1.LabelTopologyRegion,
			expectedErr:         false,
		},
		{
			name: "ExpectedBetaLabels",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelFailureDomainBetaZone:   "zone1",
							corev1.LabelFailureDomainBetaRegion: "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelFailureDomainBetaZone:   "zone2",
							corev1.LabelFailureDomainBetaRegion: "regionB",
						},
					},
				},
			},
			expectedZoneLabel:   corev1.LabelFailureDomainBetaZone,
			expectedRegionLabel: corev1.LabelFailureDomainBetaRegion,
			expectedErr:         false,
		},
		{
			name: "ExpectedPartialBetaLabels",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelFailureDomainBetaZone:   "zone1",
							corev1.LabelFailureDomainBetaRegion: "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							common.TopologyLabelsDomain + "/zone":   "zone2",
							common.TopologyLabelsDomain + "/region": "regionB",
						},
					},
				},
			},
			expectedZoneLabel:   corev1.LabelFailureDomainBetaZone,
			expectedRegionLabel: corev1.LabelFailureDomainBetaRegion,
			expectedErr:         false,
		},
		{
			name: "MixedLabelsOnNodes",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelFailureDomainBetaZone:   "zone1",
							corev1.LabelFailureDomainBetaRegion: "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelTopologyZone:   "zone2",
							corev1.LabelTopologyRegion: "regionB",
						},
					},
				},
			},
			expectedZoneLabel:   "",
			expectedRegionLabel: "",
			expectedErr:         true,
		},
		{
			name: "NoStandardLabelsOnNodes",
			nodeList: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							common.TopologyLabelsDomain + "/zone":   "zone1",
							common.TopologyLabelsDomain + "/region": "regionA",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							common.TopologyLabelsDomain + "/zone":   "zone2",
							common.TopologyLabelsDomain + "/region": "regionB",
						},
					},
				},
			},
			expectedZoneLabel:   "",
			expectedRegionLabel: "",
			expectedErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualZoneLabel, actualRegionLabel, actualErr := findExistingTopologyLabels(ctx, tt.nodeList)
			assert.Equal(t, tt.expectedErr, actualErr != nil)
			assert.Equal(t, tt.expectedZoneLabel, actualZoneLabel)
			assert.Equal(t, tt.expectedRegionLabel, actualRegionLabel)
		})
	}
}
