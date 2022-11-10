// +build !ignore_autogenerated

/*
Copyright the Velero contributors.

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

// Code generated by deepcopy-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Download) DeepCopyInto(out *Download) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Download.
func (in *Download) DeepCopy() *Download {
	if in == nil {
		return nil
	}
	out := new(Download)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *Download) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DownloadList) DeepCopyInto(out *DownloadList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Download, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DownloadList.
func (in *DownloadList) DeepCopy() *DownloadList {
	if in == nil {
		return nil
	}
	out := new(DownloadList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *DownloadList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DownloadOperationProgress) DeepCopyInto(out *DownloadOperationProgress) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DownloadOperationProgress.
func (in *DownloadOperationProgress) DeepCopy() *DownloadOperationProgress {
	if in == nil {
		return nil
	}
	out := new(DownloadOperationProgress)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DownloadSpec) DeepCopyInto(out *DownloadSpec) {
	*out = *in
	if in.RestoreTimestamp != nil {
		in, out := &in.RestoreTimestamp, &out.RestoreTimestamp
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DownloadSpec.
func (in *DownloadSpec) DeepCopy() *DownloadSpec {
	if in == nil {
		return nil
	}
	out := new(DownloadSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DownloadStatus) DeepCopyInto(out *DownloadStatus) {
	*out = *in
	if in.StartTimestamp != nil {
		in, out := &in.StartTimestamp, &out.StartTimestamp
		*out = (*in).DeepCopy()
	}
	if in.CompletionTimestamp != nil {
		in, out := &in.CompletionTimestamp, &out.CompletionTimestamp
		*out = (*in).DeepCopy()
	}
	out.Progress = in.Progress
	if in.NextRetryTimestamp != nil {
		in, out := &in.NextRetryTimestamp, &out.NextRetryTimestamp
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DownloadStatus.
func (in *DownloadStatus) DeepCopy() *DownloadStatus {
	if in == nil {
		return nil
	}
	out := new(DownloadStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Upload) DeepCopyInto(out *Upload) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Upload.
func (in *Upload) DeepCopy() *Upload {
	if in == nil {
		return nil
	}
	out := new(Upload)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *Upload) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *UploadList) DeepCopyInto(out *UploadList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Upload, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new UploadList.
func (in *UploadList) DeepCopy() *UploadList {
	if in == nil {
		return nil
	}
	out := new(UploadList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *UploadList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *UploadOperationProgress) DeepCopyInto(out *UploadOperationProgress) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new UploadOperationProgress.
func (in *UploadOperationProgress) DeepCopy() *UploadOperationProgress {
	if in == nil {
		return nil
	}
	out := new(UploadOperationProgress)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *UploadSpec) DeepCopyInto(out *UploadSpec) {
	*out = *in
	if in.BackupTimestamp != nil {
		in, out := &in.BackupTimestamp, &out.BackupTimestamp
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new UploadSpec.
func (in *UploadSpec) DeepCopy() *UploadSpec {
	if in == nil {
		return nil
	}
	out := new(UploadSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *UploadStatus) DeepCopyInto(out *UploadStatus) {
	*out = *in
	if in.StartTimestamp != nil {
		in, out := &in.StartTimestamp, &out.StartTimestamp
		*out = (*in).DeepCopy()
	}
	if in.CompletionTimestamp != nil {
		in, out := &in.CompletionTimestamp, &out.CompletionTimestamp
		*out = (*in).DeepCopy()
	}
	out.Progress = in.Progress
	if in.NextRetryTimestamp != nil {
		in, out := &in.NextRetryTimestamp, &out.NextRetryTimestamp
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new UploadStatus.
func (in *UploadStatus) DeepCopy() *UploadStatus {
	if in == nil {
		return nil
	}
	out := new(UploadStatus)
	in.DeepCopyInto(out)
	return out
}
