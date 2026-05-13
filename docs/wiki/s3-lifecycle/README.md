# Wiki sources — S3 Lifecycle

These pages are the source of truth for the [SeaweedFS GitHub wiki](https://github.com/seaweedfs/seaweedfs/wiki) under the `S3-Lifecycle-` namespace. They live here so changes are reviewed alongside the code; the wiki copies are sync'd by hand because GitHub wikis live in a separate git repo (`seaweedfs.wiki.git`).

## Pages

| File | Wiki page name |
|---|---|
| `Home.md` | `S3-Lifecycle` |
| `Operator-Guide.md` | `S3-Lifecycle-Operator-Guide` |
| `Monitoring.md` | `S3-Lifecycle-Monitoring` |
| `Troubleshooting.md` | `S3-Lifecycle-Troubleshooting` |
| `Architecture.md` | `S3-Lifecycle-Architecture` |

## Sync workflow

```sh
# one-time
git clone https://github.com/seaweedfs/seaweedfs.wiki.git ~/seaweedfs-wiki

# After editing files here, run the cp commands from the repository
# root (paths below are repo-root-relative). cd up first if you're
# editing in docs/wiki/s3-lifecycle/.
cd "$(git rev-parse --show-toplevel)"
cp docs/wiki/s3-lifecycle/Home.md            ~/seaweedfs-wiki/S3-Lifecycle.md
cp docs/wiki/s3-lifecycle/Operator-Guide.md  ~/seaweedfs-wiki/S3-Lifecycle-Operator-Guide.md
cp docs/wiki/s3-lifecycle/Monitoring.md      ~/seaweedfs-wiki/S3-Lifecycle-Monitoring.md
cp docs/wiki/s3-lifecycle/Troubleshooting.md ~/seaweedfs-wiki/S3-Lifecycle-Troubleshooting.md
cp docs/wiki/s3-lifecycle/Architecture.md    ~/seaweedfs-wiki/S3-Lifecycle-Architecture.md

cd ~/seaweedfs-wiki
git add -A && git commit -m "sync s3 lifecycle wiki from main repo" && git push
```
