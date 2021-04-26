Hi

This PR adds support for building a snap package of seaweedfs. Snaps are cross distro Linux software packages. One snap can be installed on Ubuntu all supported LTS and non LTS releases from 14.04 onward. Additionally they can installed on Debian, Manjaro, Fedora, OpenSUSE and others. Making a snap of seaweedfs enables you to provide automatic updates on your schedule to your users via the snap store.

If accepted, you can use snapcraft locally, a CI system such as travis or circle-ci, or our free build system (build.snapcraft.io) to create snaps and upload to the store (snapcraft.io/store). The store supports 

To test this PR locally, I used an Ubuntu 16.04 VM, with the following steps.

```
snap install snapcraft --classic
git clone https://github.com/popey/seaweedfs
cd seaweedfs
git checkout add-snapcraft
snapcraft
```

The generated a .snap file from the tip of master (I could have checked out a stable release instead). It can be installed with:-

    snap install seaweedfs_0.99+git30.79371c0-dirty_amd64.snap --dangerous

(the --dangerous is necessary because we’re installing an app which hasn’t gone through the snap store review process)

Once installed, the (namespaced) weed command can be executed. If you accept this and land the snap in the store, we can request an ‘alias’ so users can use the ‘weed’ command rather than the namespaced ‘seaweedfs.weed’

- Run the command
-  Create sample config. Snaps are securely confined so their home directory is in a different place
    mkdir ~/snap/seaweedfs/current/.seaweedfs
    seaweedfs.weed scaffold > ~/snap/seaweed/current/.seaweedfs/filer.toml
- Run a server
    seaweedfs.weed server
- Run a benchmark
    seaweedfs.weed benchmark

Results from my test run: https://paste.ubuntu.com/p/95Xk8zFQ7w/

If landed, you will need to:-

- Register an account in the snap store https://snapcraft.io/account
- Register the ‘seaweedfs’ name in the store 
  - snapcraft login
  - snapcraft register seaweedfs
- Upload a built snap to the store
  - snapcraft push seaweedfs_0.99+git30.79371c0-dirty_amd64.snap --release edge
- Test installing on a clean Ubuntu 16.04 machine
  - snap install seaweedfs --edge

The store supports multiple risk levels as “channels” with the edge channel typically used to host the latest build from git master. Stable is where stable releases are pushed. Optionally beta and candidate channels can also be used if needed.

Once you are happy, you can push a stable release to the stable channel, update the store page, and promote the application online (we can help there).
