/*
Copyright 2022 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/vapi/tags"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"k8s.io/kubernetes/test/e2e/framework"
)

// createVmfsStoragePolicy create a vmfs policy with given allocation type and category/tag map
func createVmfsStoragePolicy(ctx context.Context, pbmClient *pbm.Client, allocationType string,
	categoryTagMap map[string]string) (*pbmtypes.PbmProfileId, string) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	profileName := fmt.Sprintf("vmfs-policy-%v-%v", time.Now().UnixNano(), strconv.Itoa(r1.Intn(1000)))
	pbmCreateSpec := pbm.CapabilityProfileCreateSpec{
		Name:        profileName,
		Description: "VMFS test policy",
		Category:    "REQUIREMENT",
		CapabilityList: []pbm.Capability{
			{
				ID:        "VolumeAllocationType",
				Namespace: "com.vmware.storage.volumeallocation",
				PropertyList: []pbm.Property{
					{
						ID:       "VolumeAllocationType",
						Value:    allocationType,
						DataType: "string",
					},
				},
			},
		},
	}
	for k, v := range categoryTagMap {

		pbmCreateSpec.CapabilityList = append(pbmCreateSpec.CapabilityList, pbm.Capability{
			ID:        k,
			Namespace: "http://www.vmware.com/storage/tag",
			PropertyList: []pbm.Property{
				{
					ID:       "com.vmware.storage.tag." + k + ".property",
					Value:    v,
					DataType: "set",
				},
			},
		})
	}
	createSpecVMFS, err := pbm.CreateCapabilityProfileSpec(pbmCreateSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	profileID, err := pbmClient.CreateProfile(ctx, *createSpecVMFS)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("VMFS profile with id: %v and name: '%v' created", profileID.UniqueId, profileName)

	return profileID, profileName
}

// deleteStoragePolicy deletes the given storage policy
func deleteStoragePolicy(ctx context.Context, pbmClient *pbm.Client, profileID *pbmtypes.PbmProfileId) {
	_, err := pbmClient.DeleteProfile(ctx, []pbmtypes.PbmProfileId{*profileID})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// createCategoryNTag create one category and one tag associated with it
func createCategoryNTag(ctx context.Context, catName string, tagName string) (string, string) {
	tagSpec := tags.Category{Name: catName, Cardinality: "MULTIPLE"}
	mgr := newTagMgr(ctx, e2eVSphere.Client)
	catID, err := mgr.CreateCategory(ctx, &tagSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	tagID, err := mgr.CreateTag(ctx, &tags.Tag{Name: tagName, CategoryID: catID})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return catID, tagID
}

// deleteCategoryNTag deletes given category and tag
func deleteCategoryNTag(ctx context.Context, catID string, tagID string) {
	mgr := newTagMgr(ctx, e2eVSphere.Client)

	tag, err := mgr.GetTag(ctx, tagID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = mgr.DeleteTag(ctx, tag)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cat, err := mgr.GetCategory(ctx, catID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = mgr.DeleteCategory(ctx, cat)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// attachTagToDS attach given cat/tag to given datastore(URL)
func attachTagToDS(ctx context.Context, tagID string, dsURL string) {
	mgr := newTagMgr(ctx, e2eVSphere.Client)
	dsMoRef := getDsMoRefFromURL(ctx, dsURL)
	err := mgr.AttachTag(ctx, tagID, dsMoRef)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// detachTagFromDS detach given cat/tag from the given datastore(URL)
func detachTagFromDS(ctx context.Context, tagID string, dsURL string) {
	mgr := newTagMgr(ctx, e2eVSphere.Client)
	dsMoRef := getDsMoRefFromURL(ctx, dsURL)
	err := mgr.DetachTag(ctx, tagID, dsMoRef)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// getDsMoRefFromURL get datastore MoRef from its URL
func getDsMoRefFromURL(ctx context.Context, dsURL string) vim25types.ManagedObjectReference {
	dcList, err := e2eVSphere.getAllDatacenters(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ds *object.Datastore
	for _, dc := range dcList {
		ds, err = getDatastoreByURL(ctx, dsURL, dc)
		if err != nil {
			if !strings.Contains(err.Error(), "couldn't find Datastore given URL") {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			break
		}
	}
	gomega.Expect(ds).NotTo(gomega.BeNil(), "Could not find MoRef for ds URL %v", dsURL)
	return ds.Reference()
}

// createTagBasedPolicy creates a tag based storage policy with given tag and category map
func createTagBasedPolicy(ctx context.Context, pbmClient *pbm.Client,
	categoryTagMap map[string]string) (*pbmtypes.PbmProfileId, string) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	profileName := fmt.Sprintf("shared-ds-policy-%v-%v", time.Now().UnixNano(), strconv.Itoa(r1.Intn(1000)))

	pbmCreateSpec := pbm.CapabilityProfileCreateSpec{
		Name:           profileName,
		Description:    "tag based policy",
		Category:       "REQUIREMENT",
		CapabilityList: []pbm.Capability{},
	}
	//var pbmCreateSpec pbm.CapabilityProfileCreateSpec
	for k, v := range categoryTagMap {
		pbmCreateSpec.CapabilityList = append(pbmCreateSpec.CapabilityList, pbm.Capability{
			ID:        k,
			Namespace: "http://www.vmware.com/storage/tag",
			PropertyList: []pbm.Property{
				{
					ID:       "com.vmware.storage.tag." + k + ".property",
					Value:    v,
					DataType: "set",
				},
			},
		})
	}
	createTagSpec, err := pbm.CreateCapabilityProfileSpec(pbmCreateSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	profileID, err := pbmClient.CreateProfile(ctx, *createTagSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Tag based profile with id: %v and name: '%v' created", profileID.UniqueId, profileName)

	return profileID, profileName
}

// updateVmfsPolicyAlloctype updates the given policy's allocation type to given type
func updateVmfsPolicyAlloctype(
	ctx context.Context, pbmClient *pbm.Client, allocationType string, policyName string,
	policyId *pbmtypes.PbmProfileId) error {

	updateSpec := pbmtypes.PbmCapabilityProfileUpdateSpec{
		Name: policyName,
		Constraints: &pbmtypes.PbmCapabilitySubProfileConstraints{
			SubProfiles: []pbmtypes.PbmCapabilitySubProfile{
				{
					Capability: []pbmtypes.PbmCapabilityInstance{
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        "VolumeAllocationType",
								Namespace: "com.vmware.storage.volumeallocation",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id:    "VolumeAllocationType",
											Value: allocationType,
										},
									},
								},
							},
						},
					},
					Name: "volumeallocation.capabilityobjectschema.namespaceInfo.info.label rules",
				},
			},
		},
	}
	err := pbmClient.UpdateProfile(ctx, *policyId, updateSpec)
	if err != nil {
		return err
	}
	policyContent, err := pbmClient.RetrieveContent(ctx, []pbmtypes.PbmProfileId{*policyId})
	if err != nil {
		return err
	}
	framework.Logf("policy content after update", spew.Sdump(policyContent))
	return nil
}

// createVsanDStoragePolicy create a vsand storage policy with given volume allocation type and category/tag map
func createVsanDStoragePolicy(ctx context.Context, pbmClient *pbm.Client, allocationType string,
	categoryTagMap map[string]string) (*pbmtypes.PbmProfileId, string) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	profileName := fmt.Sprintf("vsand-policy-%v-%v", time.Now().UnixNano(), strconv.Itoa(r1.Intn(1000)))
	pbmCreateSpec := pbm.CapabilityProfileCreateSpec{
		Name:        profileName,
		Description: "VSAND test policy",
		Category:    "REQUIREMENT",
		CapabilityList: []pbm.Capability{
			{
				ID:        "vSANDirectVolumeAllocation",
				Namespace: "vSANDirect",
				PropertyList: []pbm.Property{
					{
						ID:       "vSANDirectVolumeAllocation",
						Value:    allocationType,
						DataType: "string",
					},
				},
			},
			{
				ID:        "vSANDirectType",
				Namespace: "vSANDirect",
				PropertyList: []pbm.Property{
					{
						ID:       "vSANDirectType",
						Value:    "vSANDirect",
						DataType: "string",
					},
				},
			},
		},
	}
	for k, v := range categoryTagMap {

		pbmCreateSpec.CapabilityList = append(pbmCreateSpec.CapabilityList, pbm.Capability{
			ID:        k,
			Namespace: "http://www.vmware.com/storage/tag",
			PropertyList: []pbm.Property{
				{
					ID:       "com.vmware.storage.tag." + k + ".property",
					Value:    v,
					DataType: "set",
				},
			},
		})
	}
	createSpecVSAND, err := pbm.CreateCapabilityProfileSpec(pbmCreateSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	profileID, err := pbmClient.CreateProfile(ctx, *createSpecVSAND)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("VSAND profile with id: %v and name: '%v' created", profileID.UniqueId, profileName)

	return profileID, profileName
}

// createStoragePolicyWithSharedVmfsNVsand create a storage policy with vmfs and vsand rules
// with given volume allocation type and category/tag map
func createStoragePolicyWithSharedVmfsNVsand(ctx context.Context, pbmClient *pbm.Client,
	allocationType string, categoryTagMap map[string]string) (*pbmtypes.PbmProfileId, string) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	var category, tag string
	profileName := fmt.Sprintf("storage-policy-%v-%v", time.Now().UnixNano(), strconv.Itoa(r1.Intn(1000)))

	for k, v := range categoryTagMap {
		category = k
		tag = v
	}
	framework.Logf("category: %v, tag: %v", category, tag)

	pbmCapabilityProfileSpec := pbmtypes.PbmCapabilityProfileCreateSpec{
		Name:        profileName,
		Description: "VSAND-VMFS test policy",
		Category:    "REQUIREMENT",
		ResourceType: pbmtypes.PbmProfileResourceType{
			ResourceType: string(pbmtypes.PbmProfileResourceTypeEnumSTORAGE),
		},
		Constraints: &pbmtypes.PbmCapabilitySubProfileConstraints{
			SubProfiles: []pbmtypes.PbmCapabilitySubProfile{
				{
					Capability: []pbmtypes.PbmCapabilityInstance{
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        "vSANDirectVolumeAllocation",
								Namespace: "vSANDirect",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id:    "vSANDirectVolumeAllocation",
											Value: allocationType,
										},
									},
								},
							},
						},
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        "vSANDirectType",
								Namespace: "vSANDirect",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id:    "vSANDirectType",
											Value: "vSANDirect",
										},
									},
								},
							},
						},
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        category,
								Namespace: "http://www.vmware.com/storage/tag",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id: "com.vmware.storage.tag." + category + ".property",
											Value: pbmtypes.PbmCapabilityDiscreteSet{
												Values: []vim25types.AnyType{tag},
											},
										},
									},
								},
							},
						},
					},
					Name: "vsandirect rules",
				},
				{
					Capability: []pbmtypes.PbmCapabilityInstance{
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        "VolumeAllocationType",
								Namespace: "com.vmware.storage.volumeallocation",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id:    "VolumeAllocationType",
											Value: allocationType,
										},
									},
								},
							},
						},
						{
							Id: pbmtypes.PbmCapabilityMetadataUniqueId{
								Id:        category,
								Namespace: "http://www.vmware.com/storage/tag",
							},
							Constraint: []pbmtypes.PbmCapabilityConstraintInstance{
								{
									PropertyInstance: []pbmtypes.PbmCapabilityPropertyInstance{
										{
											Id: "com.vmware.storage.tag." + category + ".property",
											Value: pbmtypes.PbmCapabilityDiscreteSet{
												Values: []vim25types.AnyType{tag},
											},
										},
									},
								},
							},
						},
					},
					Name: "vmfs rules",
				},
			},
		},
	}
	profileID, err := pbmClient.CreateProfile(ctx, pbmCapabilityProfileSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("VSAND and vmfs profile with id: %v and name: '%v' created", profileID.UniqueId, profileName)

	return profileID, profileName
}
