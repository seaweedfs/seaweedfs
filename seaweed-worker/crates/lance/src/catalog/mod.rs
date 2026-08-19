//! How the worker finds Lance tables and gets at their bytes.
//!
//! It goes through the Lance namespace rather than the filer: the namespace is
//! the catalog of record, it already knows which tables are Lance, and asking it
//! to describe a table with vend_credentials is how the worker gets storage
//! credentials without holding any of its own.

pub mod namespace;

pub use namespace::{NamespaceClient, TableDescription};
