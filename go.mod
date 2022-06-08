module sigs.k8s.io/vsphere-csi-driver

go 1.17

require (
	github.com/akutz/gofsutil v0.1.2
	github.com/container-storage-interface/spec v1.2.0
	github.com/davecgh/go-spew v1.1.1
	github.com/fsnotify/fsnotify v1.4.9
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.2.0
	github.com/kubernetes-csi/csi-lib-utils v0.7.0
	github.com/onsi/ginkgo v1.15.0
	github.com/onsi/gomega v1.10.5
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.9.0
	github.com/rexray/gocsi v1.2.2
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	github.com/vmware-tanzu/vm-operator-api v0.1.3
	github.com/vmware/govmomi v0.27.5
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.26.0
	gopkg.in/gcfg.v1 v1.2.3
	k8s.io/api v0.18.5
	k8s.io/apiextensions-apiserver v0.18.5
	k8s.io/apimachinery v0.18.5
	k8s.io/client-go v0.18.5
	k8s.io/kubernetes v1.18.5
	k8s.io/sample-controller v0.18.5
	k8s.io/utils v0.0.0-20200619165400-6e3d28b6ed19
	sigs.k8s.io/controller-runtime v0.6.1
)

require (
	github.com/akutz/gosync v0.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver v3.5.0+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190612170431-362f06ec6bc1 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20200710112657-153946a5f232 // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/go-logr/logr v0.1.0 // indirect
	github.com/go-logr/zapr v0.1.1 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/go-cmp v0.5.0 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/nxadm/tail v1.4.4 // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pelletier/go-toml v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.15.0 // indirect
	github.com/prometheus/procfs v0.2.0 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/thecodeteam/gofsutil v0.1.2 // indirect
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.0.0-20211019181941-9d821ace8654 // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.1.10 // indirect
	gomodules.xyz/jsonpatch/v2 v2.0.1 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20191220175831-5c49e3ecc1c1 // indirect
	google.golang.org/protobuf v1.23.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
	honnef.co/go/tools v0.2.2 // indirect
	k8s.io/apiserver v0.18.5 // indirect
	k8s.io/cloud-provider v0.18.5 // indirect
	k8s.io/component-base v0.18.5 // indirect
	k8s.io/cri-api v0.0.0 // indirect
	k8s.io/csi-translation-lib v0.18.5 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.0.0 // indirect
	k8s.io/kube-openapi v0.0.0-20200410145947-61e04a5be9a6 // indirect
	k8s.io/kube-scheduler v0.0.0 // indirect
	k8s.io/kubectl v0.0.0 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.0.7 // indirect
	sigs.k8s.io/structured-merge-diff/v3 v3.0.0 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
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
