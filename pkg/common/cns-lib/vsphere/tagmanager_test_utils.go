/*
Copyright 2024 The Kubernetes Authors.

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

package vsphere

import (
	"context"
	"fmt"

	_ "github.com/vmware/govmomi/vapi/simulator"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25/types"
)

const (
	regionCategoryName string = "k8s-region"
	regionTagName      string = "region-1"
	zoneCategoryName   string = "k8s-zone"
	zoneTagName        string = "zone-A"
)

// CreateNewCategory creates a new category using tag manager
func CreateNewCategory(ctx context.Context, categoryName string, cardinality string,
	tagManager *tags.Manager) (string, error) {
	// Create a new category
	tagSpec := tags.Category{Name: categoryName, Cardinality: cardinality}
	categoryID, err := tagManager.CreateCategory(ctx, &tagSpec)
	if err != nil {
		return "", fmt.Errorf("error creating a new tag category %s, err: %v",
			categoryName, err)
	}
	fmt.Printf("Successfully created a new category %s with categoryID %v\n",
		categoryName, categoryID)

	return categoryID, nil
}

// CreateNewTag creates a new tag within the given category
func CreateNewTag(ctx context.Context, categoryId string, tagName string,
	description string, tagManager *tags.Manager) (string, error) {
	// Create a new tag within the given category
	tagID, err := tagManager.CreateTag(ctx, &tags.Tag{Name: tagName, CategoryID: categoryId,
		Description: description})
	if err != nil {
		return "", fmt.Errorf("error creating a new tag %s within category %s, err: %v",
			tagName, categoryId, err)
	}
	fmt.Printf("Successfully created a new tag %s with tagID %v within category %s\n",
		tagName, tagID, categoryId)

	return tagID, nil
}

// AttachTag attaches a tag to the given managed object
func AttachTag(ctx context.Context, tagID string, moref types.ManagedObjectReference,
	tagManager *tags.Manager) error {
	err := tagManager.AttachTag(ctx, tagID, moref)
	if err != nil {
		return fmt.Errorf("error attaching a tag %s to moref %v, err: %v",
			tagID, moref, err)
	}
	fmt.Printf("Successfully attached a tag %s with moref %v\n", tagID, moref)

	return nil
}
