package config

import "embed"

//go:embed cns.vmware.com_storagepools.yaml
var EmbedStoragePoolCRFile embed.FS

const EmbedStoragePoolCRFileName = "cns.vmware.com_storagepools.yaml"
