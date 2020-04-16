/*
Copyright 2020 The Kubernetes Authors.

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

package manager

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	v1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

const (
	manifestPath = "/config"
)

// getCRDFromManifest reads a .json/yaml file and returns the CRD in it.
func getCRDFromManifest(ctx context.Context, fileName string) (*v1beta1.CustomResourceDefinition, error) {
	var crd v1beta1.CustomResourceDefinition
	log := logger.GetLogger(ctx)

	fullPath := filepath.Join(manifestPath, fileName)
	data, err := ioutil.ReadFile(fullPath)
	if os.IsNotExist(err) {
		log.Errorf("Manifest file: %s doesn't exist. Error: %+v", fullPath, err)
		return nil, err
	} else if err != nil {
		log.Errorf("Failed to read the manifest file: %s. Error: %+v", fullPath, err)
		return nil, err
	}

	json, err := utilyaml.ToJSON(data)
	if err != nil {
		log.Errorf("Failed to convert the manifest file: %s content to JSON with error: %+v", fullPath, err)
		return nil, err
	}

	if err := runtime.DecodeInto(legacyscheme.Codecs.UniversalDecoder(), json, &crd); err != nil {
		log.Errorf("Failed to decode json content: %+v to crd with error: %+v", json, err)
		return nil, err
	}
	return &crd, nil
}
