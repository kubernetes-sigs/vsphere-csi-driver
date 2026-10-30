#!/usr/bin/env python3

# Copyright 2026 The Kubernetes Authors.
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

"""Recover memory-watcher pprof files from logs, directories, tar bundles or ZIPs."""

import argparse
import base64
import binascii
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import sys
import tarfile
import zipfile
import zlib

PREFIX = "memory watcher profile "


def records(line):
    """Accept JSON, timestamp/CRI prefixes, and common JSON log envelopes."""
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", line):
        try:
            event, _ = decoder.raw_decode(line[match.start():])
        except ValueError:
            continue
        if not isinstance(event, dict):
            continue
        if event.get("msg", "") in (PREFIX + "begin", PREFIX + "chunk", PREFIX + "end"):
            yield event
            return
        for field in ("log", "message"):
            payload = event.get(field)
            if isinstance(payload, str) and PREFIX in payload:
                # Decode only one envelope layer, avoiding recursive input nesting.
                for nested in payload.splitlines():
                    try:
                        inner = json.loads(nested)
                    except ValueError:
                        continue
                    if isinstance(inner, dict) and inner.get("msg") in (
                        PREFIX + "begin", PREFIX + "chunk", PREFIX + "end"
                    ):
                        yield inner
                return


def scan_stream(stream, source, groups, errors, args):
    for number, raw in enumerate(stream, 1):
        line = raw.decode("utf-8", errors="replace")
        if PREFIX not in line:
            continue
        found = list(records(line))
        if not found:
            errors.append({"source": source, "line": number,
                           "error": "Unreadable profile record (possibly truncated JSON)"})
        for event in found:
            dump_id, profile = event.get("dumpId"), event.get("profile")
            trace = event.get("TraceId") or event.get("traceId")
            if not isinstance(dump_id, str) or not isinstance(profile, str):
                errors.append({"source": source, "line": number, "error": "Missing dumpId/profile"})
                continue
            if trace is not None and not isinstance(trace, str):
                errors.append({"source": source, "line": number, "error": "Invalid TraceId"})
                continue
            if args.profile and profile not in args.profile:
                continue
            if args.dump_id and dump_id not in args.dump_id:
                continue
            # TraceId is shared across a watcher's records and rotated files.
            # Without it, keep files separate rather than mixing unrelated processes.
            key = ("trace:" + trace if trace else "source:" + source, dump_id, profile)
            group = groups.setdefault(key, {"dumpId": dump_id, "profile": profile,
                                           "traceId": trace, "sources": set(),
                                           "chunks": {}, "begin": False, "end": None,
                                           "encodedBytes": 0, "errors": []})
            group["sources"].add(source)
            kind = event["msg"][len(PREFIX):]
            if kind == "begin":
                group["begin"] = True
            elif kind == "end":
                end = (event.get("chunks"), event.get("base64Bytes"))
                if group["end"] is not None and group["end"] != end:
                    group["errors"].append("Conflicting end records")
                group["end"] = end
            else:
                seq, data = event.get("seq"), event.get("data")
                if type(seq) is not int or seq < 0 or not isinstance(data, str):
                    group["errors"].append("Invalid chunk sequence/data")
                    continue
                previous = group["chunks"].get(seq)
                if previous is not None:
                    if previous != data:
                        group["errors"].append("Conflicting duplicate chunk %d" % seq)
                    continue
                if group["encodedBytes"] + len(data) > args.max_profile_mib * 1024 * 1024:
                    if "Profile exceeds size limit" not in group["errors"]:
                        group["errors"].append("Profile exceeds size limit")
                    continue
                group["chunks"][seq] = data
                group["encodedBytes"] += len(data)


def scan_member(stream, source, name, groups, errors, args):
    if name.lower().endswith(".gz"):
        with gzip.GzipFile(fileobj=stream) as uncompressed:
            scan_stream(uncompressed, source, groups, errors, args)
    else:
        scan_stream(stream, source, groups, errors, args)


def scan_file(path, groups, errors, args):
    # Archive members are streamed, never extracted to filesystem paths.
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                if member.is_dir() or stat.S_ISLNK(member.external_attr >> 16):
                    continue
                with archive.open(member) as stream:
                    scan_member(stream, str(path) + "!" + member.filename, member.filename,
                                groups, errors, args)
    elif tarfile.is_tarfile(path):
        with tarfile.open(path, "r:*") as archive:
            for member in archive:
                if member.isfile():
                    with archive.extractfile(member) as stream:
                        scan_member(stream, str(path) + "!" + member.name, member.name,
                                    groups, errors, args)
    else:
        with path.open("rb") as stream:
            scan_member(stream, str(path), path.name, groups, errors, args)


def recover(group, limit):
    if group["errors"]:
        raise ValueError("; ".join(group["errors"]))
    if not group["begin"] or group["end"] is None:
        raise ValueError("Incomplete profile: missing begin or end record")
    count, size = group["end"]
    if type(count) is not int or count <= 0 or type(size) is not int or size <= 0:
        raise ValueError("Invalid end record counts")
    chunks = group["chunks"]
    if len(chunks) != count or min(chunks, default=-1) != 0 or max(chunks, default=-1) != count - 1:
        raise ValueError("Incomplete profile: missing or unexpected chunk numbers")
    encoded = "".join(chunks[i] for i in range(count))
    if len(encoded) != size:
        raise ValueError("base64Bytes does not match reconstructed profile")
    payload = base64.b64decode(encoded, validate=True)
    # Reading through EOF checks the gzip checksum, with bounded decompression.
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        decoded = 0
        while True:
            block = stream.read(min(65536, limit + 1 - decoded))
            if not block:
                break
            decoded += len(block)
            if decoded > limit:
                raise ValueError("Decompressed profile exceeds size limit")
    if not decoded:
        raise ValueError("Empty profile")
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path, help="Log files, directories, .gz, tar/tgz or ZIP bundles")
    parser.add_argument("-o", "--output", type=Path, default=Path("."), help="Output directory for profiles and manifest.json (default: current directory)")
    parser.add_argument("--profile", action="append", help="Only extract this profile name; repeatable")
    parser.add_argument("--dump-id", action="append", help="Only extract this dumpId; repeatable")
    parser.add_argument("--max-profile-mib", type=int, default=64, help="Limit encoded and decompressed bytes per profile (default: 64 MiB)")
    args = parser.parse_args()
    if args.max_profile_mib <= 0:
        parser.error("--max-profile-mib must be positive")
    groups, errors = {}, []
    for entry in args.inputs:
        paths = sorted(entry.rglob("*")) if entry.is_dir() else [entry]
        for path in paths:
            if path.is_symlink() or path.is_dir():
                continue
            if path.resolve().parent == args.output.resolve() and (path.name == "manifest.json" or path.name.endswith(".pb.gz")):
                continue
            try:
                scan_file(path, groups, errors, args)
            except (OSError, EOFError, ValueError, tarfile.TarError, zipfile.BadZipFile, zlib.error) as error:
                errors.append({"source": str(path), "error": str(error)})
    args.output.mkdir(parents=True, exist_ok=True)
    recovered = []
    for key, group in sorted(groups.items()):
        metadata = {k: group[k] for k in ("dumpId", "profile", "traceId")}
        metadata["sources"] = sorted(group["sources"])
        try:
            payload = recover(group, args.max_profile_mib * 1024 * 1024)
            digest = hashlib.sha256(json.dumps(key).encode()).hexdigest()[:16]
            label = re.sub(r"[^A-Za-z0-9_.-]", "_", group["dumpId"] + "-" + group["profile"])[:120]
            filename = label + "-" + digest + ".pb.gz"
            target = args.output / filename
            if target.exists():
                if target.read_bytes() != payload:
                    raise ValueError("Refusing to overwrite different profile: " + filename)
            else:
                with target.open("xb") as stream:
                    stream.write(payload)
            metadata.update(file=filename, chunks=len(group["chunks"]), compressedBytes=len(payload))
            recovered.append(metadata)
        except (OSError, EOFError, ValueError, binascii.Error, zlib.error) as error:
            errors.append(dict(metadata, error=str(error)))
    manifest = {"profiles": recovered, "errors": errors}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Recovered %d profiles; %d errors. Manifest: %s" %
          (len(recovered), len(errors), args.output / "manifest.json"))
    for error in errors:
        print("ERROR: " + json.dumps(error), file=sys.stderr)
    return 0 if recovered and not errors else 1


if __name__ == "__main__":
    sys.exit(main())
