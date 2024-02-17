//! This module contains code to interact with the cache.

mod owned_archive;

use cacache::{Algorithm, Integrity};
use rkyv::{de::deserializers::SharedDeserializeMap, AlignedVec, Archive, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Read,
    ops::Deref,
    path::Path,
};
use thiserror::Error;

/// Internal representation of a cached archive.
#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedArchive {
    /// All the files stored in the archive.
    pub files: HashMap<String, CachedArchiveEntry>,

    /// Links stored in the archive.
    pub links: HashMap<String, String>,

    /// All the directories stored in the archive.
    pub directories: Vec<String>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedArchiveEntry {
    /// The content hash of the entry
    pub content: String,

    /// File permissions
    pub mode: Option<u32>,
}

#[derive(Debug, Error)]
pub enum ExtractZipArchiveError {
    /// An error occurred while reading the zip file
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),

    /// Invalid zip file entry
    #[error("{0} {1}")]
    InvalidEntry(String, String),

    /// A caching error occured
    #[error("failed to write {0} to cache")]
    CacheError(String, #[source] cacache::Error),

    /// A generic IO error occured
    #[error("failed to write {0} to cache")]
    IoError(String, #[source] std::io::Error),
}

/// Extracts the contents of a zip-archive to the cache returning a [`CachedArchive`].
///
/// The `archive_bytes` reader will be completely consumed by this function.
///
/// The [`CachedArchive`] will contain the details about all the files in the archive and how
/// they are stored in the cache.
///
/// All files in the cache will be deduplicated. Calling this function twice will yield the exact
/// same [`CachedArchive`].
///
/// Optionally, an `algorithm` can be provided. By default, this function will use
/// [`Algorithm::Xxh3`]  because it is by far the fastest. However, you are going to compute
/// different hashes anyway it might make sense to pass a different value here.
///
/// Note that the choice of algorithm affects deduplication. Only content with the same hash is
/// deduplicated this means that using another hash algorithm for the same content will result in
/// cache duplication.
pub fn extract_zip_archive(
    cache: &Path,
    archive_bytes: impl Read,
    algorithm: Option<Algorithm>,
) -> Result<CachedArchive, ExtractZipArchiveError> {
    let mut archive = CachedArchive {
        files: HashMap::new(),
        links: HashMap::new(),
        directories: Vec::new(),
    };

    // Iterate over all the files in the zip and extract them to the cache
    let mut archive_bytes = archive_bytes;
    while let Some(mut entry) = zip::read::read_zipfile_from_stream(&mut archive_bytes)
        .map_err(ExtractZipArchiveError::Zip)?
    {
        // Get the path of the entry
        let Some(path) = entry.enclosed_name() else {
            return Err(ExtractZipArchiveError::InvalidEntry(
                entry.name().to_string(),
                String::from("is not a valid path"),
            )
            .into());
        };

        // We can only store utf-8 encoded paths
        let path = path
            .as_os_str()
            .to_str()
            .ok_or_else(|| {
                ExtractZipArchiveError::InvalidEntry(
                    entry.name().to_string(),
                    String::from("is not a valid utf-8 path"),
                )
            })?
            .to_string();

        if entry.is_dir() {
            archive.directories.push(path.to_string());
        } else if entry.is_file() {
            let writer_ops =
                cacache::WriteOpts::new().algorithm(algorithm.unwrap_or(Algorithm::Xxh3));

            // Specify the last modified time if available
            let writer_ops = if let Ok(time) = entry.last_modified().to_time() {
                writer_ops.time(
                    time.unix_timestamp_nanos().checked_abs().unwrap_or(0) as u128 / 1_000_000,
                )
            } else {
                writer_ops
            };

            // Open the writer from the options
            let mut writer = writer_ops
                .open_hash_sync(cache)
                .map_err(|err| ExtractZipArchiveError::CacheError(path.to_string(), err))?;

            // Copy the file to the cache
            std::io::copy(&mut entry, &mut writer)
                .map_err(|err| ExtractZipArchiveError::IoError(path.to_string(), err))?;

            // Finish writing to the cache and get the integrity
            let integrity = writer
                .commit()
                .map_err(|err| ExtractZipArchiveError::CacheError(path.to_string(), err))?;

            // Write the entry to the archive
            archive.files.insert(
                path.to_string(),
                CachedArchiveEntry {
                    content: integrity.to_string(),
                    mode: entry.unix_mode(),
                },
            );
        }
    }

    Ok(archive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::Archived;
    use std::io::Seek;

    #[test]
    fn test_extract_zip_archive() {
        // Create a temporary cache
        let cache = tempfile::tempdir().unwrap();

        // Extract the contents of a wheel to the cache. A wheel is just a zip file.
        let wheel_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-data/wheels/miniblack-23.1.0-py3-none-any.whl");

        // Open the archive
        let mut wheel = std::fs::File::open(&wheel_path).unwrap();

        // Extract the archive to the cache
        let archive = extract_zip_archive(cache.path(), &mut wheel, None).unwrap();

        // Rewind the file and try again.
        wheel.rewind().unwrap();
        let archive2 = extract_zip_archive(cache.path(), &mut wheel, None).unwrap();

        // The two archives should be the same
        assert_eq!(&archive, &archive2);

        // Assert that the cache contains the files
        insta::assert_debug_snapshot!(archive);
    }
}
