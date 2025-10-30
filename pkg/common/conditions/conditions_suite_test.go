// © Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package conditions

import (
	_ "github.com/onsi/ginkgo/v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type nonKubeObj struct {
	c []metav1.Condition
}

func (o nonKubeObj) GetConditions() []metav1.Condition {
	return o.c
}

func (o *nonKubeObj) SetConditions(c []metav1.Condition) {
	o.c = c
}

type kubeObj corev1.Service

func (o kubeObj) GetConditions() []metav1.Condition {
	return o.Status.Conditions
}

func (o *kubeObj) SetConditions(c []metav1.Condition) {
	if o == nil {
		return
	}
	o.Status.Conditions = c
}

func (o *kubeObj) DeepCopyObject() runtime.Object {
	if o == nil {
		return nil
	}
	c := kubeObj(*((*corev1.Service)(o)).DeepCopy())
	return &c
}
