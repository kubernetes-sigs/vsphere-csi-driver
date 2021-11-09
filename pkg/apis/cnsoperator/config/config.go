package config

import "embed"

//go:embed cnsfileaccessconfig_crd.yaml
var EmbedCnsFileAccessConfigCRFile embed.FS

const EmbedCnsFileAccessConfigCRFileName = "cnsfileaccessconfig_crd.yaml"

//go:embed cnsregistervolume_crd.yaml
var EmbedCnsRegisterVolumeCRFile embed.FS

const EmbedCnsRegisterVolumeCRFileName = "cnsregistervolume_crd.yaml"
