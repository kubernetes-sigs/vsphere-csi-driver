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

package constants

const (
	ExecCommand = "/bin/df -T /mnt/volume1 | " +
		"/bin/awk 'FNR == 2 {print $2}' > /mnt/volume1/fstype && while true ; do sleep 2 ; done"
	ExecRWXCommandPod = "echo 'Hello message from Pod' > /mnt/volume1/Pod.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod.html && while true ; do sleep 2 ; done"
	ExecRWXCommandPod1 = "echo 'Hello message from Pod1' > /mnt/volume1/Pod1.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod1.html && while true ; do sleep 2 ; done"
	ExecRWXCommandPod2 = "echo 'Hello message from Pod2' > /mnt/volume1/Pod2.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod2.html && while true ; do sleep 2 ; done"
)
const (
	WindowsExecCmd = "while (1) " +
		" { Add-Content -Encoding Ascii /mnt/volume1/fstype.txt $([System.IO.DriveInfo]::getdrives() " +
		"| Where-Object {$_.DriveType -match 'Fixed'} | Select-Object -Property DriveFormat); sleep 1 }"
	WindowsExecRWXCommandPod = "while (1) " +
		" { Add-Content /mnt/volume1/Pod.html 'Hello message from Pod'; sleep 1 }"
	WindowsExecRWXCommandPod1 = "while (1) " +
		" { Add-Content /mnt/volume1/Pod1.html 'Hello message from Pod1'; sleep 1 }"
)
