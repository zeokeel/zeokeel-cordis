#!/usr/bin/env python3
"""Generate a minimal CycloneDX 1.5 JSON SBOM for this crate.

Uses only `cargo metadata` output so it works in any CI without extra tools.
The kernel ships source and a tiny dependency set; the SBOM records the exact
resolved versions (and the Cargo.lock checksum when available).
"""

import hashlib
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def run_metadata() -> dict:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout)


def lock_checksums() -> dict[tuple[str, str], str]:
    """Parse Cargo.lock and return `(name, version) -> checksum`.

    Registry packages carry the real crates.io checksum; local/path packages do
    not, and are skipped. This is the authoritative artifact hash an SBOM
    consumer expects, unlike a synthetic hash of "name version".
    """
    lock = ROOT / "Cargo.lock"
    if not lock.exists():
        return {}
    checksums: dict[tuple[str, str], str] = {}
    name = version = None
    in_package = False
    for line in lock.read_text().splitlines():
        line = line.strip()
        if line == "[[package]]":
            in_package = True
            name = version = None
            continue
        if not in_package:
            continue
        match = re.fullmatch(r'name = "(.+)"', line)
        if match:
            name = match.group(1)
            continue
        match = re.fullmatch(r'version = "(.+)"', line)
        if match:
            version = match.group(1)
            continue
        match = re.fullmatch(r'checksum = "(.+)"', line)
        if match and name and version:
            checksums[(name, version)] = match.group(1)
    return checksums


def main() -> int:
    metadata = run_metadata()
    root = ROOT
    # Traverse the resolved dependency graph from this crate and keep only
    # normal/build dependencies (dev-dependencies are test-only and do not ship
    # with the kernel).
    package_ids = {p["id"]: p for p in metadata["packages"]}
    resolve = metadata.get("resolve") or {}
    root_ids = set(metadata.get("workspace_members") or [])
    keep: set[str] = set()
    queue = [(root_id, "normal") for root_id in root_ids]
    while queue:
        current, _ = queue.pop()
        for dep in resolve.get("nodes", []):
            if dep["id"] != current:
                continue
            for child in dep.get("deps", []):
                kinds = child.get("dep_kinds") or [{"kind": None}]
                if not any(kind.get("kind") in (None, "build") for kind in kinds):
                    continue
                if child["pkg"] in keep:
                    continue
                keep.add(child["pkg"])
                queue.append((child["pkg"], "normal"))
            break
    checksums = lock_checksums()
    components = []
    for package in metadata["packages"]:
        # Workspace members are identified by id: a CARGO_HOME living inside the
        # repository must not make every registry package look "local".
        if package["id"] in root_ids or package["id"] not in keep:
            continue
        purl = f"pkg:cargo/{package['name']}@{package['version']}"
        checksum = checksums.get((package["name"], package["version"]))
        component = {
            "type": "library",
            "name": package["name"],
            "version": package["version"],
            "purl": purl,
        }
        if checksum:
            component["hashes"] = [{"alg": "SHA-256", "content": checksum}]
        components.append(component)
    lockfile = ROOT / "Cargo.lock"
    metadata_hashes = []
    if lockfile.exists():
        metadata_hashes.append(
            {
                "alg": "SHA-256",
                "content": hashlib.sha256(lockfile.read_bytes()).hexdigest(),
            }
        )
    project = next(p for p in metadata["packages"] if p["id"] in root_ids)
    document = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "component": {
                "type": "library",
                "name": project["name"],
                "version": project["version"],
                "purl": f"pkg:cargo/{project['name']}@{project['version']}",
                "hashes": metadata_hashes,
            }
        },
        "components": sorted(components, key=lambda c: (c["name"], c["version"])),
    }
    out_path = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "target" / "sbom.cdx.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(document, indent=2) + "\n")
    print(f"wrote {out_path} ({len(components)} components)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
