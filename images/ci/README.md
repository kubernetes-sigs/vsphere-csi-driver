# Continuous integration

The image `gcr.io/cloud-provider-vsphere/csi-ci` is used by Prow jobs to build, test, and deploy the CSI provider.

## The CI workflow

Prow jobs are configured to perform the following steps:

| Job type | Linters | Build binaries | Unit test | Build images | Integration test | Deploy images | Conformance test |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Presubmit | ✓ | ✓ | ✓ | ✓ | | | |
| Postsubmit | ✓ | ✓ | ✓ | ✓ | | ✓ | |

## Up-to-date sources

When running on Prow the jobs map the current sources into the CI container. That may be simulated locally by running the examples from a directory containing the desired sources and providing the `docker run` command with the following flags:

* `-v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver`

## Docker-in-Docker

Several of the jobs require Docker-in-Docker. To mimic that locally there are two options:

1. [Provide the host's Docker to the container](#provide-the-hosts-docker-to-the-container)
2. [Run the Docker server inside the container](#run-the-docker-server-inside-the-container)

### Provide the host's Docker to the container

While Prow jobs [run the Docker server inside the container](#run-the-docker-server-inside-the-container), this option provides a low-cost (memory, disk) solution for testing locally. This option is enabled by running the examples from a directory containing the desired sources and providing the `docker run` command with the following flags:

* `-v /var/run/docker.sock:/var/run/docker.sock`
* `-v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver`

Please note that this option is only available when using a local copy of the sources. This is because all of the paths known to Docker will be of the local host system, not from the container.

### Run the Docker server inside the container

This is option that Prow jobs utilize and is also the method illustrated by the examples below. Please keep in mind that using this option locally requires a large amount of memory and disk space available to Docker:

| Type | Minimum Requirement |
|------|---------------------|
| Memory | 8GiB |
| Disk | 200GiB |

For Windows and macOS systems this means adjusting the size of the Docker VM disk and the amount of memory the Docker VM is allowed to use.

Resources notwithstanding, running the Docker server inside the container also requires providing the `docker run` command with the following flags:

* `--privileged`

## Check the sources

To check the sources run the following command:

```shell
$ docker run -it --rm \
  -e "ARTIFACTS=/out" -v "$(pwd)":/out \
  -v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver \
  gcr.io/cloud-provider-vsphere/csi-ci \
  make check
```

The above command will create the following files in the working directory:

* `junit_check.xml`

## Build the CSI binary

The CI image is built with Go module and build caches from a recent build of the project's `master` branch. Therefore the CI image can be used to build the CSI binary in a matter of seconds:

```shell
$ docker run -it --rm \
  -e "BIN_OUT=/out" -v "$(pwd)":/out \
  -v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver \
  gcr.io/cloud-provider-vsphere/csi-ci \
  make build
```

The above command will create the following files in the working directory:

* `vsphere-csi.linux_amd64`

## Execute the unit tests

```shell
$ docker run -it --rm \
  -v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver \
  gcr.io/cloud-provider-vsphere/csi-ci \
  make unit-test
```

## Build the CSI image

Building the CSI image inside another image requires Docker-in-Docker (DinD):

```shell
$ docker run -it --rm --privileged \
  -v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver \
  gcr.io/cloud-provider-vsphere/csi-ci \
  make build-images
```

## Deploy the CSI image

Pushing the images requires bind mounting a GCR key file into the container and setting the environment variable `GCR_KEY_FILE` to inform the deployment process the location of the key file:

```shell
$ docker run -it --rm --privileged \
  -v "$(pwd)":/go/src/sigs.k8s.io/vsphere-csi-driver \
  -e "GCR_KEY_FILE=/keyfile.json" -v "$(pwd)/keyfile.json":/keyfile.json \
  gcr.io/cloud-provider-vsphere/csi-ci \
  make push-images
```
