module sigs.k8s.io/vsphere-csi-driver

go 1.13

require (
	github.com/akutz/gofsutil v0.1.2
	github.com/container-storage-interface/spec v1.2.0
	github.com/coreos/etcd v3.3.18+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20190421051319-9d40249d3c2f // indirect
	github.com/elazarl/goproxy/ext v0.0.0-20190421051319-9d40249d3c2f // indirect
	github.com/fatih/camelcase v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-logr/zapr v0.1.1 // indirect
	github.com/go-openapi/spec v0.19.3
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.12.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kubernetes-csi/csi-lib-utils v0.7.0
	github.com/onsi/ginkgo v1.12.0
	github.com/onsi/gomega v1.8.1
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pborman/uuid v1.2.0 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.1.0 // indirect
	github.com/prometheus/procfs v0.0.4 // indirect
	github.com/rexray/gocsi v1.2.1
	github.com/thecodeteam/gofsutil v0.1.2 // indirect
	github.com/vmware-tanzu/vm-operator-api v0.1.0
	github.com/vmware/govmomi v0.22.2-0.20200329172045-a01612b98505
	github.com/zekroTJA/timedmap v0.0.0-20191029102728-85f9d45635d7
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b
	golang.org/x/sys v0.0.0-20200331124033-c3d80250170d // indirect
	golang.org/x/tools v0.0.0-20200407191807-cd5a53e07f8a // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
	k8s.io/api v0.17.4
	k8s.io/apiextensions-apiserver v0.17.2
	k8s.io/apimachinery v0.17.4
	k8s.io/apiserver v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/cli-runtime v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/client-go v0.17.2
	k8s.io/cloud-provider v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/cluster-bootstrap v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/csi-translation-lib v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/kube-aggregator v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a
	k8s.io/kubernetes v1.14.10
	k8s.io/sample-controller v0.0.0-00010101000000-000000000000
	sigs.k8s.io/controller-runtime v0.5.0
	sigs.k8s.io/kustomize v2.0.3+incompatible // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20191212015246-8fe0c124fb40
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/apiserver => k8s.io/apiserver v0.0.0-20191212015046-43d571094e6f
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20191212015340-98374817910c
	k8s.io/client-go => k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191212015549-86a326830157
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.0.0-20191212015531-26b810c03ac1
	k8s.io/code-generator => k8s.io/code-generator v0.0.0-20190311093542-50b561225d70
	k8s.io/component-base => k8s.io/component-base v0.0.0-20191212015026-7bd5511f1bb2
	k8s.io/cri-api => k8s.io/cri-api v0.15.8-beta.1
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20191212015623-92af21758231
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20191212015114-c9678d254875
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.0.0-20191212015514-df3f0d40afbb
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.0.0-20191212015419-57e0fd8078bb
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.0.0-20191212015456-bbb33a8f263f
	k8s.io/kubectl => k8s.io/kubectl v0.15.8-beta.1.0.20190801031749-f16387a69211
	k8s.io/kubelet => k8s.io/kubelet v0.0.0-20191212015437-d70ac875e65f
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.0.0-20200108002541-bb65d835785c
	k8s.io/metrics => k8s.io/metrics v0.0.0-20191212015306-529b54bfdff7
	k8s.io/node-api => k8s.io/node-api v0.0.0-20191212015607-f3cb4356c0e0
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.0.0-20191212015145-bbdf84b5bbd8
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.0.0-20191212015401-7495faa875a6
	k8s.io/sample-controller => k8s.io/sample-controller v0.0.0-20191212015209-bc15e564ed1e
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.3.0
)
