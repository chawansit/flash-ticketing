# SWR image publication

[Current candidate](candidate-096f68d73618-2026-10-07.json) is published and verified, not deployed.
Application source is Git revision 096f68d7361809a3b42cc3dc32b0474b6211b179.
Documentation commits after it do not change that image's application revision.

Use the manifest's immutable_reference for deployment. Its tag is a lookup label;
never substitute latest. Keep local_image_id, image_config_digest and
manifest_digest separate: Docker Desktop may expose the manifest as its local ID.

Build only a tracked revision exported from Git, with the recorded base digest
and locked dependencies. The verified SWR-compatible builder settings are:

```text
--platform linux/amd64 --provenance=false --sbom=false
--output=type=docker,oci-mediatypes=false
```

Include the full revision in org.opencontainers.image.revision. Run tests inside
the built image and compare installed source hashes before publication. Retain
build recipe, dependency, configuration and manifest hashes in a new receipt.
Authenticate through password stdin and the credential store. Never place login
passwords in commands, Docker build arguments, source files or public evidence.

Future deployment must check every running API and worker against the same
manifest, including startup configuration and mounted source. Digest equality
alone cannot validate configuration or code hidden by mounts. Roll back using the
previous recorded digest. Current cloud recovery still blocks further load;
publication does not approve deployment or qualify ticket throughput.

The SWR compatibility fix removes attached provenance/SBOM attestations; the
independent publication manifest retains source and build evidence. See
[ADR0215](../../adr/0215-digest-pinned-swr-image-publication.md).
