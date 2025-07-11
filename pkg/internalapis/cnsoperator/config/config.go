package config

import "embed"

//go:embed cnsfilevolumeclient_crd.yaml
var EmbedCnsFileVolumeClientFile embed.FS

const EmbedCnsFileVolumeClientFileName = "cnsfilevolumeclient_crd.yaml"

//go:embed cnssharedblockvolumeinfo_crd.yaml
var EmbedCnsSharedBlockVolumeInfoFile embed.FS

const EmbedCnsSharedBlockVolumeInfoFileName = "cnssharedblockvolumeinfo_crd.yaml"

//go:embed triggercsifullsync_crd.yaml
var EmbedTriggerCsiFullSync embed.FS

const EmbedTriggerCsiFullSyncName = "triggercsifullsync_crd.yaml"
