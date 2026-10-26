//! Vector queries over the Lance tables the SeaweedFS namespace registers.
//!
//! A table buckets rows carry a key column, a fixed-size float vector column,
//! and metadata columns. This crate resolves that layout and runs top-K over
//! the whole dataset: index-accelerated where an ANN index exists, exact where
//! it does not, always covering the fragments no index has reached yet.
