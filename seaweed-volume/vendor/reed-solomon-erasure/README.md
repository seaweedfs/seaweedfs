# reed-solomon-erasure
[![Build Status](https://travis-ci.org/darrenldl/reed-solomon-erasure.svg?branch=master)](https://travis-ci.org/darrenldl/reed-solomon-erasure)
[![Build status](https://ci.appveyor.com/api/projects/status/47c0emjoa9bhpjlb/branch/master?svg=true)](https://ci.appveyor.com/project/darrenldl/reed-solomon-erasure/branch/master)
[![codecov](https://codecov.io/gh/darrenldl/reed-solomon-erasure/branch/master/graph/badge.svg)](https://codecov.io/gh/darrenldl/reed-solomon-erasure)
[![Coverage Status](https://coveralls.io/repos/github/darrenldl/reed-solomon-erasure/badge.svg?branch=master)](https://coveralls.io/github/darrenldl/reed-solomon-erasure?branch=master)
[![Crates](https://img.shields.io/crates/v/reed-solomon-erasure.svg)](https://crates.io/crates/reed-solomon-erasure)
[![Documentation](https://docs.rs/reed-solomon-erasure/badge.svg)](https://docs.rs/reed-solomon-erasure)
[![dependency status](https://deps.rs/repo/github/darrenldl/reed-solomon-erasure/status.svg)](https://deps.rs/repo/github/darrenldl/reed-solomon-erasure)

Rust implementation of Reed-Solomon erasure coding

WASM builds are also available, see section **WASM usage** below for details

This is a port of [BackBlaze's Java implementation](https://github.com/Backblaze/JavaReedSolomon), [Klaus Post's Go implementation](https://github.com/klauspost/reedsolomon), and [Nicolas Trangez's Haskell implementation](https://github.com/NicolasT/reedsolomon).

Version `1.X.X` copies BackBlaze's implementation, and is less performant as there were fewer places where parallelism could be added.

Version `>= 2.0.0` copies Klaus Post's implementation. The SIMD C code is copied from Nicolas Trangez's implementation with minor modifications.

See [Notes](#notes) and [License](#license) section for details.

## WASM usage

See [here](wasm/README.md) for details

## Rust usage
Add the following to your `Cargo.toml` for the normal version (pure Rust version)
```toml
[dependencies]
reed-solomon-erasure = "4.0"
```
or the following for the version which tries to utilise SIMD
```toml
[dependencies]
reed-solomon-erasure = { version = "4.0", features = [ "simd-accel" ] }
```
and the following to your crate root
```rust
extern crate reed_solomon_erasure;
```

NOTE: `simd-accel` is tuned for Haswell+ processors on x86-64 and not in any way for other architectures, set
environment variable `RUST_REED_SOLOMON_ERASURE_ARCH` during build to force compilation of C code for specific architecture (`-march` flag in
GCC/Clang). Even on x86-64 you can achieve better performance by setting it to `native`, but it will stop running on
older CPUs, YMMV.

## Example
```rust
#[macro_use(shards)]
extern crate reed_solomon_erasure;

use reed_solomon_erasure::galois_8::ReedSolomon;
// or use the following for Galois 2^16 backend
// use reed_solomon_erasure::galois_16::ReedSolomon;

fn main () {
    let r = ReedSolomon::new(3, 2).unwrap(); // 3 data shards, 2 parity shards

    let mut master_copy = shards!(
        [0, 1,  2,  3],
        [4, 5,  6,  7],
        [8, 9, 10, 11],
        [0, 0,  0,  0], // last 2 rows are parity shards
        [0, 0,  0,  0]
    );

    // Construct the parity shards
    r.encode(&mut master_copy).unwrap();

    // Make a copy and transform it into option shards arrangement
    // for feeding into reconstruct_shards
    let mut shards: Vec<_> = master_copy.iter().cloned().map(Some).collect();

    // We can remove up to 2 shards, which may be data or parity shards
    shards[0] = None;
    shards[4] = None;

    // Try to reconstruct missing shards
    r.reconstruct(&mut shards).unwrap();

    // Convert back to normal shard arrangement
    let result: Vec<_> = shards.into_iter().filter_map(|x| x).collect();

    assert!(r.verify(&result).unwrap());
    assert_eq!(master_copy, result);
}
```

## Benchmark it yourself
You can test performance under different configurations quickly (e.g. data parity shards ratio, parallel parameters)
by cloning this repo: https://github.com/darrenldl/rse-benchmark

`rse-benchmark` contains a copy of this library (usually a fully functional dev version), so you only need to adjust `main.rs`
then do `cargo run --release` to start the benchmark.

## Performance
Version `1.X.X`, `2.0.0` do not utilise SIMD.

Version `2.1.0` onward uses Nicolas's C files for SIMD operations.

Machine: laptop with `Intel(R) Core(TM) i5-3337U CPU @ 1.80GHz (max 2.70GHz) 2 Cores 4 Threads`

Below shows the result of one of the test configurations, other configurations show similar results in terms of ratio.

|Configuration| Klaus Post's | >= 2.1.0 && < 4.0.0 | 2.0.X | 1.X.X |
|---|---|---|---|---|
| 10x2x1M | ~7800MB/s |~4500MB/s | ~1000MB/s | ~240MB/s |

Versions `>= 4.0.0` have not been benchmarked thoroughly yet

## Changelog
[Changelog](CHANGELOG.md)

## Contributions
Contributions are welcome. Note that by submitting contributions, you agree to license your work under the same license used by this project as stated in the LICENSE file.

## Credits
#### Library overhaul and Galois 2^16 backend
Many thanks to the following people for overhaul of the library and introduction of Galois 2^16 backend

  - [@drskalman](https://github.com/drskalman)

  - Jeff Burdges [@burdges](https://github.com/burdges)

  - Robert Habermeier [@rphmeier](https://github.com/rphmeier)

#### WASM builds
Many thanks to Nazar Mokrynskyi [@nazar-pc](https://github.com/nazar-pc) for submitting his package for WASM builds

He is the original author of the files stored in `wasm` folder. The files may have been modified by me later.

#### AVX512 support
Many thanks to [@sakridge](https://github.com/sakridge) for adding support for AVX512 (see [PR #69](https://github.com/darrenldl/reed-solomon-erasure/pull/69))

#### build.rs improvements
Many thanks to [@ryoqun](https://github.com/ryoqun) for improving the usability of the library in the context of cross-compilation (see [PR #75](https://github.com/darrenldl/reed-solomon-erasure/pull/75))

#### no_std support
Many thanks to Nazar Mokrynskyi [@nazar-pc](https://github.com/nazar-pc) for adding `no_std` support (see [PR #90](https://github.com/darrenldl/reed-solomon-erasure/pull/90))

#### Testers
Many thanks to the following people for testing and benchmarking on various platforms

  - Laurențiu Nicola [@lnicola](https://github.com/lnicola/) (platforms: Linux, Intel)

  - Roger Andersen [@hexjelly](https://github.com/hexjelly) (platforms: Windows, AMD)

## Notes
#### Code quality review
If you'd like to evaluate the quality of this library, you may find audit comments helpful.

Simply search for "AUDIT" to see the dev notes that are aimed at facilitating code reviews.

#### Implementation notes
The `1.X.X` implementation mostly copies [BackBlaze's Java implementation](https://github.com/Backblaze/JavaReedSolomon).

`2.0.0` onward mostly copies [Klaus Post's Go implementation](https://github.com/klauspost/reedsolomon), and copies C files from [Nicolas Trangez's Haskell implementation](https://github.com/NicolasT/reedsolomon).

The test suite for all versions copies [Klaus Post's Go implementation](https://github.com/klauspost/reedsolomon) as basis.

## License
#### Nicolas Trangez's Haskell Reed-Solomon implementation
The C files for SIMD operations are copied (with no/minor modifications) from [Nicolas Trangez's Haskell implementation](https://github.com/NicolasT/reedsolomon), and are under the same MIT License as used by NicolasT's project

#### TL;DR
All files are released under the MIT License
