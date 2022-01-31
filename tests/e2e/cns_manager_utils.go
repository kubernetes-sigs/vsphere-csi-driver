/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	cnsutils "sigs.k8s.io/vsphere-csi-driver/v2/tests/e2e/cns-manager"
)

//getFCDPerDatastore method gives FCD IDs on Datastore
func getFCDPerDatastore(cnsHost string, oAuth string, datacenter string,
	datastore string) (cnsutils.DatastoreResourcesResult, error) {
	ginkgo.By("Get FCD for Datastore")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/datastoreresources"
	q := url.Values{}
	q.Add("datacenter", datacenter)
	q.Add("datastore", datastore)

	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var datastoreResults cnsutils.DatastoreResourcesResult
	err = json.Unmarshal(response, &datastoreResults)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Response is %v", datastoreResults)

	return datastoreResults, err
}

//suspendvolumeprovisioning method suspends volume provisioning on specified datastore
func suspendvolumeprovisioning(cnsHost string, oAuth string, datacenter string, datastore string,
	isNegative bool) (cnsutils.SuspendResumeVolumeProvisioningResult, int, error) {
	ginkgo.By("Get FCD for Datastore")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/suspendvolumeprovisioning"
	q := url.Values{}
	q.Add("datacenter", datacenter)
	q.Add("datastore", datastore)
	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("POST", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)

	var suspendResumeResult cnsutils.SuspendResumeVolumeProvisioningResult
	if !isNegative {
		gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

		err = json.Unmarshal(response, &suspendResumeResult)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Response is %v", suspendResumeResult)

		return suspendResumeResult, statusCode, err
	} else {
		return suspendResumeResult, statusCode, err
	}
}

//resumevolumeprovisioning method resumes volume provisioning on specified datastore
func resumevolumeprovisioning(cnsHost string, oAuth string, datacenter string, datastore string,
	isNegative bool) (cnsutils.SuspendResumeVolumeProvisioningResult, int, error) {
	ginkgo.By("Get FCD for Datastore")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/resumevolumeprovisioning"
	q := url.Values{}
	q.Add("datacenter", datacenter)
	q.Add("datastore", datastore)
	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("POST", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)

	var suspendResumeResult cnsutils.SuspendResumeVolumeProvisioningResult
	if !isNegative {
		gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

		err = json.Unmarshal(response, &suspendResumeResult)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Response is %v", suspendResumeResult)

		return suspendResumeResult, statusCode, err
	} else {
		return suspendResumeResult, statusCode, err
	}
}

//migrateVolumes method gives FCD IDs on Datastore
func migrateVolumes(cnsHost string, oAuth string, datacenter string, srcdatastore string,
	targetDatastore string, fcdIdsToMigrate string, isNegative bool) (cnsutils.MigrateVolumesResult, int, error) {
	ginkgo.By("Migrate volume on FCD for Datastore")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/migratevolumes"
	q := url.Values{}
	q.Add("datacenter", datacenter)
	q.Add("sourceDatastore", srcdatastore)
	q.Add("targetDatastore", targetDatastore)
	q.Add("skipPolicyCheck", "true")
	if fcdIdsToMigrate != "" {
		q.Add("fcdIdsToMigrate", fcdIdsToMigrate)
	}

	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("POST", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)
	var migrateVolumesResult cnsutils.MigrateVolumesResult
	if !isNegative {
		gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 202))

		err = json.Unmarshal(response, &migrateVolumesResult)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Response is %v", migrateVolumesResult)

		return migrateVolumesResult, statusCode, err
	} else {
		return migrateVolumesResult, statusCode, err
	}
}

//getJobStatus method gives FCD IDs on Datastore
func getJobStatus(cnsHost string, oAuth string, jobId string) (cnsutils.JobResult, error) {
	time.Sleep(3 * time.Second)
	ginkgo.By(fmt.Sprintf("Invoking getJobStatus on jobID %s", jobId))
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/getjobstatus"
	q := url.Values{}
	q.Add("jobId", jobId)

	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var jobResult cnsutils.JobResult
	err = json.Unmarshal(response, &jobResult)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Response is %v", jobResult)

	return jobResult, err
}

//waitforjob method waits till the migraton job completes
func waitforjob(cnsHost string, oAuth string, jobId string) (cnsutils.JobResult, error) {
	time.Sleep(2 * time.Second)
	ginkgo.By(fmt.Sprintf("Invoking waitforjob on jobID %s", jobId))
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := cnsHost + "/waitforjob"
	q := url.Values{}
	q.Add("jobId", jobId)

	framework.Logf("URL %v", getGCURL)

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.URL.RawQuery = q.Encode()
	req.Header.Add("Cookie", oAuth)
	req.Header.Add("accept", "application/json")
	response, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var jobResult cnsutils.JobResult
	err = json.Unmarshal(response, &jobResult)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Response is %v", jobResult)

	return jobResult, err
}

// Phase waits for the filesystem in the pv to be resized
func waitForMigrationToBePhase(cnshost string, oauth string, jobId string) (cnsutils.JobResult, error) {
	var jobResult cnsutils.JobResult
	var err error
	waitErr := wait.PollImmediate(pollTimeout, totalResizeWaitPeriod, func() (bool, error) {
		jobResult, err = getJobStatus(cnshost, oauth, jobId)
		if err != nil {
			return false, fmt.Errorf("error fetching migration status : %v", err)
		}

		framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
		framework.Logf("Migrate of volume status is %+v", jobResult.JobStatus.VolumeMigrationJobStatus)

		//If pvc's status field size is greater than or equal to pvc's size then done
		if jobResult.JobStatus.OverallPhase == "Queued" ||
			jobResult.JobStatus.OverallPhase == "Running" ||
			jobResult.JobStatus.OverallPhase == "Success" {
			return true, nil
		}
		return false, nil
	})
	return jobResult, waitErr
}

// Phase waits for the filesystem in the pv to be resized
func waitForMigrationToComplete(cnshost string, oauth string, jobId string) (cnsutils.JobResult, error) {
	var jobResult cnsutils.JobResult
	var err error
	waitErr := wait.PollImmediate(resizePollInterval, totalResizeWaitPeriod, func() (bool, error) {
		jobResult, err = getJobStatus(cnshost, oauth, jobId)
		if err != nil {
			return false, fmt.Errorf("error fetching migration status : %v", err)
		}

		framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
		framework.Logf("Migrate of volume status is %+v", jobResult.JobStatus.VolumeMigrationJobStatus)

		if jobResult.JobStatus.OverallPhase == "Success" {
			for _, fcdMigrationTask := range jobResult.JobStatus.VolumeMigrationTasks {
				if fcdMigrationTask.Phase != "Success" {
					return true, errors.New("OverAll status is completed but, some of FCD migration is failed")
				}
			}
			return true, nil
		}
		return false, nil
	})
	return jobResult, waitErr
}
