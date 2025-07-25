# vSphere Shared Session capability

One problem that can be found when provisioning a large amount of clusters using
vSphere CSI is vCenter session exhaustion. This happens because every
workload cluster needs to request a new session to vSphere to do proper reconciliation.

vSphere 8.0U3 and up uses a new approach of session management, that allows the
creation and sharing of the sessions among different clusters.

A cluster admin can implement a rest API that, once called, requests a new vCenter
session and shares with CSI. This session will not count on the total generated
sessions of vSphere, and instead will be a child derived session.

This configuration can be applied on vSphere CSI with the usage of
the following CSI configuration:

```shell
[Global]
ca-file = "/etc/ssl/certs/trusted-certificates.crt"
[VirtualCenter "your-vcenter-host"]
datacenters = "datacenter1"
vc-session-manager-url = "https://some-session-manager/session"
vc-session-manager-token = "a-secret-token"
```

The configuration above will make CSI call the shared session rest API and use the
provided token to authenticate against vSphere, instead of using a username/password.

The parameter provider at `vc-session-manager-token` is sent as a `Authorization: Bearer` token
to the session manager, and in case this directive is not configured CSI will send the
Pod Service Account token instead.

Below is an example implementation of a shared session manager rest API. Starting the
program below and calling `http://127.0.0.1:18080/session` should return a JSON that is expected
by CSI using session manager to work:

```shell
$ curl 127.0.0.1:18080/session
{"token":"cst-VCT-52f8d061-aace-4506-f4e6-fca78293a93f-....."}
```

**NOTE**: Below implementation is **NOT PRODUCTION READY** and does not implement
any kind of authentication!

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "net/url"

    "github.com/vmware/govmomi"
    "github.com/vmware/govmomi/session"
    "github.com/vmware/govmomi/vim25"
    "github.com/vmware/govmomi/vim25/soap"
)

const (
    vcURL      = "https://my-vc.tld"
    vcUsername = "Administrator@vsphere.local"
    vcPassword = "somepassword"
)

var (
    userPassword = url.UserPassword(vcUsername, vcPassword)
)

// SharedSessionResponse is the expected response of CPI when using Shared session manager
type SharedSessionResponse struct {
    Token string `json:"token"`
}

func main() {
    ctx := context.Background()
    vcURL, err := soap.ParseURL(vcURL)
    if err != nil {
        panic(err)
    }
    soapClient := soap.NewClient(vcURL, false)
    c, err := vim25.NewClient(ctx, soapClient)
    if err != nil {
        panic(err)
    }
    client := &govmomi.Client{
        Client:         c,
        SessionManager: session.NewManager(c),
    }
    if err := client.SessionManager.Login(ctx, userPassword); err != nil {
        panic(err)
    }

    vcsession := func(w http.ResponseWriter, r *http.Request) {
        clonedtoken, err := client.SessionManager.AcquireCloneTicket(ctx)
        if err != nil {
            w.WriteHeader(http.StatusForbidden)
            return
        }
        token := &SharedSessionResponse{Token: clonedtoken}
        jsonT, err := json.Marshal(token)
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            return
        }
        w.WriteHeader(http.StatusOK)
        w.Write(jsonT)
    }

    http.HandleFunc("/session", vcsession)
    log.Printf("starting webserver on port 18080")
    http.ListenAndServe(":18080", nil)
}
```
