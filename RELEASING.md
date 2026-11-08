# Releasing

## Versions

Before 1.0, a version is `0.<phase>.<patch>`. The minor number is the phase of the [build brief](BRIEF.md)
that the release completes or previews, and the patch number counts fixes within it. For example, `0.4.0`
is the cluster preview.

- Every 0.x release is published as a GitHub **pre-release**. The protocol and the history format may change
  between minor versions until phase 5 freezes them.
- Cluster servers accept peers of the same major version whose minor version differs by at most one
  (`cluster::compatible`). A rolling upgrade can therefore move one minor version at a time, for example from
  0.4 to 0.5 but not from 0.4 to 0.6.
- The Rust workspace and the Python and TypeScript SDKs share one version.

## Steps

1. On a branch, set the version in all three places:
   - `Cargo.toml` (`[workspace.package]`), then run `cargo update -w` to update `Cargo.lock`;
   - `sdks/python/pyproject.toml`;
   - `sdks/typescript/package.json`.
2. Write the release notes in `docs/releases/v<version>.md`. Cover what changed, what is not done, and any
   format or protocol change a user must know about.
3. Open a pull request. Merge it once CI passes.
4. Tag the merge commit on `main` and push the tag:

   ```sh
   git switch main && git pull
   git tag -a v<version> -m "Aqueduct v<version>"
   git push origin v<version>
   ```

5. The [release workflow](.github/workflows/release.yml) does the rest:
   - it checks that the tag matches the workspace version and that the notes exist;
   - it builds `aqueduct` for Linux on x86-64, Linux on ARM64 and macOS on Apple silicon;
   - it publishes the release with each archive, a SHA-256 checksum for each, and the notes.

   If it fails, fix the cause on `main`, delete the tag (`git push origin :v<version>`) and tag again. Once
   a release is published, its tag stays: publish a new patch version instead.
