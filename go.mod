module sigs.k8s.io/vsphere-csi-driver

go 1.12

require (
	cloud.google.com/go v0.45.1 // indirect
	github.com/Azure/go-autorest v13.0.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.9.0 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/akutz/gofsutil v0.1.2
	github.com/akutz/gosync v0.1.0 // indirect
	github.com/akutz/memconn v0.1.0
	github.com/container-storage-interface/spec v1.0.0
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.15+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20190711103511-473e67f1d7d2 // indirect
	github.com/emicklei/go-restful v2.9.6+incompatible // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/go-openapi/spec v0.19.2 // indirect
	github.com/go-openapi/swag v0.19.5 // indirect
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/google/pprof v0.0.0-20190723021845-34ac40c74b70 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gophercloud/gophercloud v0.4.0 // indirect
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/mailru/easyjson v0.0.0-20190626092158-b2ccc519800e // indirect
	github.com/munnerz/goautoneg v0.0.0-20190414153302-2ae31c8b6b30 // indirect
	github.com/onsi/ginkgo v1.10.1
	github.com/onsi/gomega v1.7.0
	github.com/pkg/errors v0.8.1 // indirect
	github.com/prometheus/client_golang v1.1.0 // indirect
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/procfs v0.0.4 // indirect
	github.com/rexray/gocsi v1.0.0
	github.com/rogpeppe/go-internal v1.3.1 // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/stretchr/testify v1.4.0 // indirect
	github.com/thecodeteam/gofsutil v0.1.2 // indirect
	github.com/thecodeteam/gosync v0.1.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/vmware/govmomi v0.21.1-0.20190821201433-8bdc2d6fc858
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.opencensus.io v0.22.1 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/crypto v0.0.0-20190829043050-9756ffdc2472 // indirect
	golang.org/x/exp v0.0.0-20190829153037-c13cbed26979 // indirect
	golang.org/x/image v0.0.0-20190902063713-cb417be4ba39 // indirect
	golang.org/x/mobile v0.0.0-20190830201351-c6da95954960 // indirect
	golang.org/x/net v0.0.0-20190827160401-ba9fcec4b297 // indirect
	golang.org/x/sys v0.0.0-20190904154756-749cb33beabd // indirect
	golang.org/x/tools v0.0.0-20190906203814-12febf440ab1 // indirect
	google.golang.org/api v0.10.0 // indirect
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/genproto v0.0.0-20190905072037-92dd089d5514 // indirect
	google.golang.org/grpc v1.23.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3 // indirect
	k8s.io/api v0.0.0-20190905160310-fb749d2f1064
	k8s.io/apimachinery v0.0.0-20190831074630-461753078381
	k8s.io/client-go v0.0.0-20190819141724-e14f31a72a77
	k8s.io/gengo v0.0.0-20190826232639-a874a240740c // indirect
	k8s.io/klog v0.4.0
	k8s.io/sample-controller v0.0.0-20180822125000-be98dc6210ab
	k8s.io/utils v0.0.0-20190829053155-3a4a5477acf8 // indirect
	sigs.k8s.io/structured-merge-diff v0.0.0-20190820212518-960c3cc04183 // indirect
)
