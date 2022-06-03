module sigs.k8s.io/vsphere-csi-driver

go 1.16

require (
	github.com/akutz/gofsutil v0.1.2
	github.com/container-storage-interface/spec v1.2.0
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20200710112657-153946a5f232 // indirect
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-logr/zapr v0.1.1 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/google/go-cmp v0.5.0 // indirect
	github.com/google/uuid v1.2.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.5 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kubernetes-csi/csi-lib-utils v0.7.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/ginkgo v1.15.0
	github.com/onsi/gomega v1.10.5
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.9.0
	github.com/rexray/gocsi v1.2.2
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/thecodeteam/gofsutil v0.1.2 // indirect
	github.com/vmware-tanzu/vm-operator-api v0.1.3
	github.com/vmware/govmomi v0.27.5
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.10 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
	honnef.co/go/tools v0.2.2 // indirect
	k8s.io/api v0.18.5
	k8s.io/apiextensions-apiserver v0.18.5
	k8s.io/apimachinery v0.18.5
	k8s.io/client-go v0.18.5
	k8s.io/kubernetes v1.18.5
	k8s.io/sample-controller v0.18.5
	k8s.io/utils v0.0.0-20200619165400-6e3d28b6ed19
	sigs.k8s.io/controller-runtime v0.6.1
	sigs.k8s.io/controller-tools v0.2.5 // indirect
)

replace (
	github.com/go-logr/logr => github.com/go-logr/logr v0.1.0
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.0
	k8s.io/api => k8s.io/api v0.18.5
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.18.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.5
	k8s.io/apiserver => k8s.io/apiserver v0.18.5
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.18.5
	k8s.io/client-go => k8s.io/client-go v0.18.5
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.18.5
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.18.5
	k8s.io/code-generator => k8s.io/code-generator v0.18.5
	k8s.io/component-base => k8s.io/component-base v0.18.5
	k8s.io/cri-api => k8s.io/cri-api v0.18.5
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.18.5
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.18.5
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.18.5
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.18.5
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.18.5
	k8s.io/kubectl => k8s.io/kubectl v0.18.5
	k8s.io/kubelet => k8s.io/kubelet v0.18.5
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.18.5
	k8s.io/metrics => k8s.io/metrics v0.18.5
	k8s.io/node-api => k8s.io/node-api v0.18.5
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.18.5
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.18.5
	k8s.io/sample-controller => k8s.io/sample-controller v0.18.5
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.6.1
)
