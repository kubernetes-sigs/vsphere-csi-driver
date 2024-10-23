# Copyright 2023 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To build an image from this docker file follow the below steps
# cd to images/ci/e2e folder
# docker build -t <image-name:version> --platform=Linux/x86_64 -f Dockerfile . 
# docker tag <image-name:version> <repo>/<image-name:version>
# docker push <repo>/<image-name:version>

FROM --platform=linux/amd64 golang:1.23-bullseye

RUN apt-get update && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

RUN echo "Downloading and installing govc..." && \
    wget -qO- https://github.com/vmware/govmomi/releases/download/v0.30.2/govc_Linux_x86_64.tar.gz | tar -C /usr/local/bin -xvzf - govc && \
    chmod +x /usr/local/bin/govc

RUN echo "Updating and installing sshpass..." && \
    apt-get update -y && \
    apt-get install -y sshpass
