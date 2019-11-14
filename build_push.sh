make build-images && \
	docker tag vsphere-csi-driver/vsphere-csi:latest docker.io/sandeeppissay/vsphere-csi:$1 && \
	docker tag vsphere-csi-driver/syncer:latest docker.io/sandeeppissay/syncer:$1 && \
        docker push docker.io/sandeeppissay/vsphere-csi:$1 && \
	docker push docker.io/sandeeppissay/syncer:$1
