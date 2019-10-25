# Running e2e Tests

The section outlines how to setup the VCenter, Datastores and env variables for running e2e test on different cluster flavors

## Vanilla Cluster

Here are the detailed steps on how to run e2e tests on [Vanilla k8s](docs/vanilla_cluster_setup.md)

## Topology Aware Vanilla Cluster

Here are the detailed steps on how to run e2e tests on [Topology aware Vanilla k8s](docs/topology_aware_vanilla_setup.md)

## Requirements

* Ginkgo in your PATH for running end-to-end tests

### Run e2e tests

    make test-e2e

### Running specific e2e test

To run a particular e2e test, set GINKGO_FOCUS to the string located “ginkgo.It()” for that test:
To run the Disk Size test located at (./vsphere_volume_disksize.go))

    export GINKGO_FOCUS="Volume\sDisk\sSize"

Note that specify spaces using “\s”.
