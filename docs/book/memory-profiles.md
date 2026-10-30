# Collect memory profiles from logs

The CSI driver and syncer log `heap`, `goroutine`, and `allocs` profiles when live Go heap reaches 95% of `GOMEMLIMIT` after a GC cycle. Repeated dumps are limited to once every **five minutes**. `GOMEMLIMIT` must be set; this measures Go heap, not total container memory.

The feature is on by default and can be turned off with `--enable-profile-logger=false`. When the live heap stays at or above 80% of `GOMEMLIMIT` for one minute after GC, a single `memory watcher: live heap did not drop below the elevated level after GC` warning is logged per episode, without profiles, so the timing of a climb can be matched against other log lines.

## What appears in the logs

When the threshold is reached, look for:

1. `memory watcher: live heap reached threshold, writing profiles to the log`, with `liveHeapBytes` and `thresholdBytes`.
2. For each profile, `memory watcher profile begin`, numbered `memory watcher profile chunk` records containing base64 `data`, and `memory watcher profile end` with chunk and byte counts.
3. `memory watcher: wrote profiles` when the dump finishes, or `memory watcher: writing profiles failed` if it fails.

Records include `TraceId`, `dumpId`, and `profile` to keep processes and dumps separate. Preserve INFO logs and all chunks; truncated or rotated-away logs cannot reconstruct a complete profile.

## Collect and extract

Collect logs from the affected pod and container:

```sh
kubectl -n vmware-system-csi logs <pod-name> -c vsphere-csi-controller > controller.log
kubectl -n vmware-system-csi logs <pod-name> -c vsphere-syncer > syncer.log
```

Add `--previous` for a restarted container if its previous logs are still available. A sudden OOM kill may interrupt a dump.

Run the [extraction script](../../hack/extract-memory-profiles.py) with Python 3:

```sh
python3 hack/extract-memory-profiles.py controller.log syncer.log -o recovered-profiles
python3 hack/extract-memory-profiles.py support-bundle.tar.gz -o recovered-profiles
```

Inputs can be log files, gzip logs, extracted directories, tar/tgz bundles, or ZIP bundles. Gzipped log members are supported; unpack nested archives first. Use `--profile heap` to extract only heap profiles. Output goes to the current directory unless `-o` is given.

Each profile is limited to 64 MiB, both as encoded base64 in the logs and as decompressed data. Use `--max-profile-mib` to change the limit. A profile over the limit is not written; it is listed under `errors` in `manifest.json`, and the script exits nonzero.

To recover one profile by hand, select its chunk lines from JSON logs and decode them:

```sh
jq -r 'select(.msg=="memory watcher profile chunk" and .profile=="heap" and
  .dumpId=="<id>") | .data' controller.log | tr -d '\n' | base64 -d > heap.pb.gz
```

The output contains `.pb.gz` profiles and `manifest.json` listing their sources and any errors. The script checks chunk sequences, byte counts, base64, and gzip integrity. It preserves valid profiles even if others are incomplete and exits nonzero on errors or no matching profiles. Rotated files are combined by `TraceId`; without it, files are kept separate.

## Inspect the profiles

Choose a filename from `manifest.json`:

```sh
go tool pprof -top recovered-profiles/<heap-profile>.pb.gz
```

If the Go installation lacks `pprof`, use the standalone version pinned by this branch:

```sh
go run github.com/google/pprof@v0.0.0-20260402051712-545e8a4df936 \
  -top recovered-profiles/<heap-profile>.pb.gz
```

Large `inuse_space` entries in the heap profile identify retained allocations. The `allocs` profile shows cumulative allocation activity, and the `goroutine` profile shows goroutine stacks.
