package cnsunregistervolume

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

func TestGetSnapshotsForPVC(t *testing.T) {
	containerOrchestratorUtilityOriginal := commonco.ContainerOrchestratorUtility
	defer func() {
		commonco.ContainerOrchestratorUtility = containerOrchestratorUtilityOriginal
	}()

	t.Run("WhenContainerOrchestratorIsNotInitialised", func(tt *testing.T) {
		// Setup
		commonco.ContainerOrchestratorUtility = nil
		expErr := errors.New("ContainerOrchestratorUtility is not initialized")

		// Execute
		_, _, err := getSnapshotsForPVC(context.Background(), "", "")

		// Assert
		assert.Equal(tt, expErr, err)
	})

	t.Run("WhenPVCHasNoSnapshots", func(tt *testing.T) {
		// Setup
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		// Execute
		snaps, inUse, err := getSnapshotsForPVC(context.Background(), "no-snaps", "")

		// Assert
		assert.NoError(tt, err)
		assert.False(tt, inUse)
		assert.Empty(tt, snaps)
	})

	t.Run("WhenPVCHasSnapshots", func(tt *testing.T) {
		// Setup
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		expSnaps := []string{"snap1", "snap2", "snap3"}

		// Execute
		snaps, inUse, err := getSnapshotsForPVC(context.Background(), "with-snaps", "")

		// Assert
		assert.NoError(tt, err)
		assert.True(tt, inUse)
		assert.Equal(tt, expSnaps, snaps)
	})
}
