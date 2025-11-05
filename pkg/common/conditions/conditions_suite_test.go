/*
Copyright 2025 The Kubernetes Authors.

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

//nolint:all
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
