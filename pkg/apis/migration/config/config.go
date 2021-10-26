package config

import "embed"

//go:embed cns.vmware.com_cnsvspherevolumemigrations.yaml
var EmbedCnsVSphereVolumeMigrationFile embed.FS

const EmbedCnsVSphereVolumeMigrationFileName = "cns.vmware.com_cnsvspherevolumemigrations.yaml"
