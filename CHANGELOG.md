# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [1.0.7](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.6...v1.0.7) (2024-01-25)


### Bug Fixes

* allow_failure ([1dfba01](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/1dfba013c5c8282ae1dba2954ee054f17f4c7a7b))
* s3tests to release ([4461c5c](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/4461c5ccbcf5c9793a99cbbf9fec100fbfbae552))

### [1.0.6](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.5...v1.0.6) (2024-01-25)


### Bug Fixes

* clean metric MasterReplicaPlacementMismatch for unregister volume ([e7f2580](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/e7f258066289dbb36e7ccb9f335acf566572feac))

### [1.0.5](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.4...v1.0.5) (2024-01-25)


### Minor Changes

* build image s3test ([a2a9a6c](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/a2a9a6ca215031e165bb4a281160f00ceacb8ddb))

### [1.0.4](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.3...v1.0.4) (2024-01-24)


### Minor Changes

* add build witout tag 5BytesOffset ([f280a41](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/f280a41dd1a87678fed075cdcf73c6287d04a4f9))

### [1.0.3](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.2...v1.0.3) (2024-01-24)

### [1.0.2](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.1...v1.0.2) (2024-01-23)


### Bug Fixes

* added debug logging for loop exit to SubscribeLocalMetadata ([c913d06](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/c913d06d2c9f300d98605b8e53e45b21431cd211))

### 1.0.1 (2024-01-23)


### Bug Fixes

* **k8s-chart-helm:** `helm upgrade` statefulset error ([#5207](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/issues/5207)) ([53be97d](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/53be97d5bea8b44db40077c62af5e224e21be3ee))
* Revert "fix write volume over size MaxPossibleVolumeSize ([#5190](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/issues/5190))" ([7048f11](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/7048f110b3e4226a88ccdf8d7eb96ecf231fdd03))
* skip s3 .uploads ([be166b4](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/be166b434f002dba684e8e3630a1e87fdb6a96db))


### Minor Changes

* add ci/cd ([410e2b9](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/410e2b9827464fd64d6254de78d06ad7211b7514))
* filer healthz handler check filer store ([#5208](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/issues/5208)) ([4e9ea1e](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/4e9ea1e628a4d2105a758d12673efdeebe43ebaa))
* fix typos in scaffold help output ([#5211](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/issues/5211)) ([2eb8277](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/2eb82778bc85474ef71e5c923780dd141c12856f))

### [1.0.7](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.6...v1.0.7) (2024-01-17)


### Minor Changes

* add debug logging ([1fdf433](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/1fdf433e5b0e38df768a71ca5c31472a87b13f9c))

### [1.0.6](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.5...v1.0.6) (2024-01-12)


### Minor Changes

* add logging for setOffset ([b836b86](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/b836b86cd1ef40c5a1f5a05b415071c499b017d0))

### [1.0.5](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.4...v1.0.5) (2024-01-12)


### Minor Changes

* refactor filer sync setoffset ([d3023db](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/d3023dbaa6eb131d3078d6c360db0c028096d812))

### [1.0.4](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.3...v1.0.4) (2024-01-05)


### Minor Changes

* filer sync add doDeleteFiles option for create only mode ([1b45007](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/1b45007f51c02ba2b756126b7c719fe2cd8cc63b))

### [1.0.3](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.2...v1.0.3) (2024-01-05)


### Bug Fixes

* upload asset name for ci ([6479958](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/647995893efeddbbe15d934b490e2729c69a6de4))

### [1.0.2](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.1...v1.0.2) (2024-01-05)


### Bug Fixes

* typo asset name for ci ([418a6f9](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/418a6f9c3f59b751976eff46e27008740ea7da98))

### [1.0.1](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/compare/v1.0.0...v1.0.1) (2024-01-05)

## 1.0.0 (2024-01-04)


### Bug Fixes

* add buildvcs for build large disk ([e7758b3](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/e7758b362ab33b7fa4c3c14b18a630ceff821795))
* add wg ([32cd548](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/32cd548e9d63a7f13688085a2eff9b054aec95f2))
* build large disk ([fde5c57](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/fde5c571732cacf6dfc4d9f524fc403d276cb04d))
* increase speed cmd fs meta load ([5ce76e8](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/5ce76e8152d5583a13e92de4ec4ce98061e8a1bd))
* make build ([f98eaaa](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/f98eaaac006a33c9d0914d9a71d0090f956e991d))
* name to amd64 ([53de2c1](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/53de2c125bfc221aa1a1e9722283639998c8c88a))
* print err in fs.meta.load ([7b797e0](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/7b797e047a56aafe2697227da9252f185684028e))
* push dir ([bfaed2e](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/bfaed2e62d69502769d3f65206bf6eaabcc4b1f0))
* work dir for build large disk ([0bbf0b0](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/0bbf0b07645206fa41a8db368dbeb71daae378ee))
* work dir for build large disk ([c9a9b5e](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/c9a9b5e60d8a5a834d403840b5039a93c110861e))


### Minor Changes

* **tests:** update S3 compat tests ([ef20d5f](https://gitlab.stripchat.dev/infrastructure/utils/storage/seaweedfs/commit/ef20d5fc635f033d05eb6e116064e260160ffeba))
