# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Security

- Bind the unauthenticated `weed mini` Admin UI/API and worker gRPC control
  plane to loopback by default. Authenticated deployments can retain network
  access, while explicit flags support legacy or remote-worker configurations
  ([#11612](https://github.com/seaweedfs/seaweedfs/issues/11612),
  [#11613](https://github.com/seaweedfs/seaweedfs/pull/11613)).
