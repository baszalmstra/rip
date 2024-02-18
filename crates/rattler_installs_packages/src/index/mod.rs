//! This module contains functions for working with PyPA packaging repositories.

mod file_store;

mod direct_url;
mod git_interop;
pub mod html;
mod http;
mod package_database;
mod package_sources;
mod lazy_metadata;

pub use package_database::{ArtifactRequest, PackageDb};
pub use package_sources::{PackageSources, PackageSourcesBuilder};

pub use self::http::CacheMode;
pub use html::parse_hash;
