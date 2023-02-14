package config

import "embed"

//go:embed datamover.cnsdp.vmware.com_downloads.yaml
var DataMoverDownloadFile embed.FS

const DataMoverDownloadFileName = "datamover.cnsdp.vmware.com_downloads.yaml"

//go:embed datamover.cnsdp.vmware.com_uploads.yaml
var DataMoverUploadFile embed.FS

const DataMoverUploadFileName = "datamover.cnsdp.vmware.com_uploads.yaml"
