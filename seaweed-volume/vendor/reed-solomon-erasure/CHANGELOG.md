## 6.0.0
- Use LruCache instead of InversionTree for caching data decode matrices
  - See [PR #104](https://github.com/rust-rse/reed-solomon-erasure/pull/104)
- Minor code duplication
  - See [PR #102](https://github.com/rust-rse/reed-solomon-erasure/pull/102)
- Dependencies update
  - Updated `smallvec` from `0.6.1` to `1.8.0`

## 5.0.3
- Fixed cross build bug for aarch64 with simd-accel
  - See [PR #100](https://github.com/rust-rse/reed-solomon-erasure/pull/100)

## 5.0.2
* Add support for `RUST_REED_SOLOMON_ERASURE_ARCH` environment variable and stop using `native` architecture for SIMD code
  - See [PR #98](https://github.com/rust-rse/reed-solomon-erasure/pull/98)

## 5.0.1
- The `simd-accel` feature now builds on M1 Macs
  - See [PR #92](https://github.com/rust-rse/reed-solomon-erasure/pull/92)
- Minor code cleanup

## 5.0.0
- Merged several PRs
- Not fully reviewed as I am no longer maintaining this crate

## 4.0.2
- Updated build.rs to respect RUSTFLAGS's target-cpu if available
  - See [PR #75](https://github.com/darrenldl/reed-solomon-erasure/pull/75)
- Added AVX512 support
  - See [PR #69](https://github.com/darrenldl/reed-solomon-erasure/pull/69)
- Disabled SIMD acceleration when MSVC is being used to build the library
  - See [PR #67](https://github.com/darrenldl/reed-solomon-erasure/pull/67)
- Dependencies update
  - Updated `smallvec` from `0.6` to `1.2`

## 4.0.1
- Updated SIMD C code for Windows compatibility
  - Removed include of `unistd.h` in `simd_c/reedsolomon.c`
  - Removed GCC `nonnull` attribute in `simd_c/reedsolomon.h`
  - See PR [#63](https://github.com/darrenldl/reed-solomon-erasure/pull/63) [#64](https://github.com/darrenldl/reed-solomon-erasure/pull/64) for details
- Replaced use of `libc::uint8_t` in `src/galois_8.rs` with `u8`

## 4.0.0
- Major API restructure: removed `Shard` type in favor of generic functions
- The logic of this crate is now generic over choice of finite field
- The SIMD acceleration feature for GF(2^8) is now activated with the `simd-accel` Cargo feature. Pure-rust behavior is default.
- Ran rustfmt
- Adds a GF(2^16) implementation

## 3.1.2 (not published)
- Doc fix
  - Added space before parantheses in code comments and documentation
- Disabled SIMD C code for Android and iOS targets entirely

## 3.1.1
- Fixed `Matrix::augment`
  - The error checking code was incorrect
  - Since this method is used in internal code only, and the only use case is a correct use case, the error did not lead to any bugs
- Fixed benchmark data
  - Previously used MB=10^6 bytes while I should have used MB=2^20 bytes
  - Table in README has been updated accordingly
    - The `>= 2.1.0` data is obtained by measuring again with the corrected `rse-benchmark` code
    - The `2.0.X` and `1.X.X` data are simply adjusted by mutiplying `10^6` then dividing by `2^20`
- Dependencies update
  - Updated `rand` from `0.4` to `0.5.4`
- Added special handling in `build.rs` for CC options on Android and iOS
  - `-march=native` is not available for GCC on Android, see issue #23

## 3.1.0
- Impl'd `std::error::Error` for `reed_solomon_erasure::Error` and `reed_solomon_erasure::SBSError`
  - See issue [#17](https://github.com/darrenldl/reed-solomon-erasure/issues/17), suggested by [DrPeterVanNostrand](https://github.com/DrPeterVanNostrand)
- Added fuzzing suite
  - No code changes due to this as no bugs were found
- Upgraded InversionTree QuickCheck test
  - No code changes due to this as no bugs were found
- Upgraded test suite for main codec methods (e.g. encode, reconstruct)
  - A lot of heavy QuickCheck tests were added
  - No code changes due to this as no bugs were found
- Upgraded test suite for ShardByShard methods
  - A lot of heavy QuickCheck tests were added
  - No code changes due to this as no bugs were found
- Minor code refactoring in `reconstruct_internal` method
  - This means `reconstruct` and related methods are slightly more optimized

## 3.0.3
- Added QuickCheck tests to the test suite
  - InversionTree is heavily tested now
- No code changes as no bugs were found
- Deps update
  - Updated rayon from 0.9 to 1.0

## 3.0.2
- Same as 3.0.1, but 3.0.1 had unapplied changes

## 3.0.1 (yanked)
- Updated doc for `with_buffer` variants of verifying methods
  - Stated explicitly that the buffer contains the correct parity shards after a successful call
- Added tests for the above statement

## 3.0.0
- Added `with_buffer` variants for verifying methods
  - This gives user the option of reducing heap allocation(s)
- Core code clean up, improvements, and review, added more AUDIT comments
- Improved shard utils
- Added code to remove leftover parity shards in `reconstruct_data_shards`
  - This means one fewer gotcha of using the methods
- `ShardByShard` code review and overhaul
- `InversionTree` code review and improvements

## 2.4.0
- Added more flexibility for `convert_2D_slices` macro
  - Now accepts expressions rather than just identifiers
  - The change requires change of syntax

## 2.3.3
- Replaced all slice splitting functions in `misc_utils` with std lib ones or rayon ones
  - This means there are fewer heap allocations in general

## 2.3.2
- Made `==`(`eq`) for `ReedSolomon` more reasonable
  - Previously `==` would compare
    - data shard count
    - parity shard count
    - total shard count
    - internal encoding matrix
    - internal `ParallelParam`
  - Now it only compares
    - data shard count
    - parity shard count

## 2.3.1
- Added info on encoding behaviour to doc

## 2.3.0
- Made Reed-Solomon codec creation methods return error instead of panic when shard numbers are not correct

## 2.2.0
- Fixed SBS error checking code
- Documentation fixes and polishing
- Renamed `Error::InvalidShardsIndicator` to `Error::InvalidShardFlags`
- Added more details to documentation on error handling
- Error handling code overhaul and checks for all method variants
- Dead commented out code cleanup and indent fix

## 2.1.0
- Added Nicolas's SIMD C code files, gaining major speedup on supported CPUs
- Added support for "shard by shard" encoding, allowing easier streamed encoding
- Added functions for shard by shard encoding

## 2.0.0
- Complete rewrite of most code following Klaus Post's design
- Added optimsations (parallelism, loop unrolling)
- 4-5x faster than `1.X.X`

## 1.1.1
- Documentation polish
- Added documentation badge to README
- Optimised internal matrix related operations
  - This largely means `decode_missing` is faster

## 1.1.0
- Added more helper functions
- Added more tests

## 1.0.1
- Added more tests
- Fixed decode_missing
  - Previously may reconstruct the missing shards with incorrect length

## 1.0.0
- Added more tests
- Added integration with Codecov (via kcov)
- Code refactoring
- Added integration with Coveralls (via kcov)

## 0.9.1
- Code restructuring
- Added documentation

## 0.9.0
- Base version
