// //go:build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CnsVolumeOperationRequest) DeepCopyInto(out *CnsVolumeOperationRequest) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CnsVolumeOperationRequest.
func (in *CnsVolumeOperationRequest) DeepCopy() *CnsVolumeOperationRequest {
	if in == nil {
		return nil
	}
	out := new(CnsVolumeOperationRequest)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *CnsVolumeOperationRequest) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CnsVolumeOperationRequestList) DeepCopyInto(out *CnsVolumeOperationRequestList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]CnsVolumeOperationRequest, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CnsVolumeOperationRequestList.
func (in *CnsVolumeOperationRequestList) DeepCopy() *CnsVolumeOperationRequestList {
	if in == nil {
		return nil
	}
	out := new(CnsVolumeOperationRequestList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *CnsVolumeOperationRequestList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CnsVolumeOperationRequestSpec) DeepCopyInto(out *CnsVolumeOperationRequestSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CnsVolumeOperationRequestSpec.
func (in *CnsVolumeOperationRequestSpec) DeepCopy() *CnsVolumeOperationRequestSpec {
	if in == nil {
		return nil
	}
	out := new(CnsVolumeOperationRequestSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CnsVolumeOperationRequestStatus) DeepCopyInto(out *CnsVolumeOperationRequestStatus) {
	*out = *in
	in.FirstOperationDetails.DeepCopyInto(&out.FirstOperationDetails)
	if in.LatestOperationDetails != nil {
		in, out := &in.LatestOperationDetails, &out.LatestOperationDetails
		*out = make([]OperationDetails, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CnsVolumeOperationRequestStatus.
func (in *CnsVolumeOperationRequestStatus) DeepCopy() *CnsVolumeOperationRequestStatus {
	if in == nil {
		return nil
	}
	out := new(CnsVolumeOperationRequestStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *OperationDetails) DeepCopyInto(out *OperationDetails) {
	*out = *in
	in.TaskInvocationTimestamp.DeepCopyInto(&out.TaskInvocationTimestamp)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new OperationDetails.
func (in *OperationDetails) DeepCopy() *OperationDetails {
	if in == nil {
		return nil
	}
	out := new(OperationDetails)
	in.DeepCopyInto(out)
	return out
}
