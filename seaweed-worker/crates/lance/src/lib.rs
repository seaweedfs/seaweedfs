//! Maintenance jobs for Lance tables.
//!
//! A Lance dataset needs three things done to it over time: its fragments
//! compacted, its indices extended to cover rows written after they were built,
//! and its old versions removed. None can run in the Go worker, because all
//! three read and rewrite Lance files. This crate is the worker that can.

pub mod catalog;
pub mod dataset;
pub mod jobs;
pub mod metrics;
pub mod preview;

pub use jobs::handlers;
