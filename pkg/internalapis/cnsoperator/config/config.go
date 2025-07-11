package config

import "embed"

//go:embed cnsfilevolumeclient_crd.yaml
var EmbedCnsFileVolumeClientFile embed.FS

const EmbedCnsFileVolumeClientFileName = "cnsfilevolumeclient_crd.yaml"

//go:embed cnsvolumeattachment_crd.yaml
var EmbedCnsVolumeAttachmentFile embed.FS

const EmbedCnsVolumeAttachmentFileName = "cnsvolumeattachment_crd.yaml"

//go:embed triggercsifullsync_crd.yaml
var EmbedTriggerCsiFullSync embed.FS

const EmbedTriggerCsiFullSyncName = "triggercsifullsync_crd.yaml"
