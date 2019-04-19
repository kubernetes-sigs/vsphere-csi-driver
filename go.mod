module sigs.k8s.io/vsphere-csi-driver

go 1.12

require (
	github.com/akutz/gofsutil v0.1.2
	github.com/akutz/memconn v0.1.0
	github.com/container-storage-interface/spec v1.0.0
	github.com/google/btree v1.0.0 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/gregjones/httpcache v0.0.0-20190212212710-3befbb6ad0cc // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/onsi/ginkgo v1.8.0
	github.com/onsi/gomega v1.4.3
	github.com/rexray/gocsi v0.4.1-0.20181205192803-207653674028
	github.com/sirupsen/logrus v1.4.1
	github.com/vmware/govmomi v0.20.0
	golang.org/x/net v0.0.0-20190415214537-1da14a5a36f2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/grpc v1.19.0
	k8s.io/cloud-provider-vsphere v0.1.2-0.20190402145035-39e2b5e81d7e
	k8s.io/klog v0.2.0
	k8s.io/kubernetes v1.11.2
)
