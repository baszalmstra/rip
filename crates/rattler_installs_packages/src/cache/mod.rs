//! This module contains code to interact with the cache.

mod owned_archive;

use cacache::Algorithm;
use data_encoding::BASE64URL_NOPAD;
use rattler_digest::Sha256;
use rkyv::{Archive, Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, path::Path};
use thiserror::Error;

/// Information about a cached wheel.
#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedWheel {
    /// All the files stored in the archive.
    /// The order must be deterministic to guarentee the same content hash.
    /// TODO: I couldnt use IndexMap here because rkyv only supports indexmap v1.
    pub files: BTreeMap<String, CachedWheelEntry>,

    /// All the directories stored in the archive.
    pub directories: Vec<String>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedWheelEntry {
    /// The content hash of the entry. This can be used to look up the entry in
    /// the cache.
    pub content: String,

    /// The SHA256 hash of the entry. This is used to compare against the
    /// RECORD file stored in the wheel. The string is in the same format as
    /// in the RECORD file e.g.:
    /// `"sha256=xxxx"`
    pub sha256: String,

    /// File permissions
    pub mode: Option<u32>,
}

#[derive(Debug, Error)]
pub enum ExtractWheelError {
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

/// Extracts the contents of a wheel to the cache returning a [`CachedWheel`]. A
/// wheel is simply a zip archive.
///
/// The `archive_bytes` reader will be completely consumed by this function.
///
/// The [`CachedWheel`] will contain the details about all the files in the archive and how
/// they are stored in the cache.
///
/// All files in the cache will be deduplicated. Calling this function twice will yield the exact
/// same [`CachedWheel`].
///
/// This function always uses the Xhh3 hashing algorithm to store files in the cache. The reason is
/// to enable more deduplication by using the best hash possible to cache the artifacts. Xhh3 is
/// extremely fast, much faster than sha256, which makes it the logical choice to use to store the
/// data in the cache. However, we also compute the sha256 hash of each file to make sure we can
/// compare it to the RECORD file stored in the wheel.
pub fn extract<R: std::io::Read>(
    cache: &Path,
    byte_stream: R,
) -> Result<CachedWheel, ExtractWheelError> {
    let mut archive = CachedWheel {
        files: BTreeMap::new(),
        directories: Vec::new(),
    };

    // Iterate over all the files in the zip and extract them to the cache
    let mut archive_bytes = byte_stream;
    while let Some(mut entry) =
        zip::read::read_zipfile_from_stream(&mut archive_bytes).map_err(ExtractWheelError::Zip)?
    {
        // Get the path of the entry
        let Some(path) = entry.enclosed_name() else {
            return Err(ExtractWheelError::InvalidEntry(
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
                ExtractWheelError::InvalidEntry(
                    entry.name().to_string(),
                    String::from("is not a valid utf-8 path"),
                )
            })?
            .to_string();

        if entry.is_dir() {
            archive.directories.push(path.to_string());
        } else if entry.is_file() {
            let writer_ops = cacache::WriteOpts::new().algorithm(Algorithm::Xxh3);

            // Specify the last modified time if available
            let writer_ops = if let Ok(time) = entry.last_modified().to_time() {
                writer_ops.time(
                    time.unix_timestamp_nanos().checked_abs().unwrap_or(0) as u128 / 1_000_000,
                )
            } else {
                writer_ops
            };

            // Open the writer from the options
            let writer = writer_ops
                .open_hash_sync(cache)
                .map_err(|err| ExtractWheelError::CacheError(path.to_string(), err))?;

            // While extracting compute the sha256 hash at the same time.
            let mut writer = rattler_digest::HashingWriter::<_, Sha256>::new(writer);

            // Copy the file to the cache
            std::io::copy(&mut entry, &mut writer)
                .map_err(|err| ExtractWheelError::IoError(path.to_string(), err))?;

            // Finish the SHA256 computation
            let (writer, sha256) = writer.finalize();

            // Finish writing to the cache and get the integrity
            let integrity = writer
                .commit()
                .map_err(|err| ExtractWheelError::CacheError(path.to_string(), err))?;

            // Write the entry to the archive
            archive.files.insert(
                path.to_string(),
                CachedWheelEntry {
                    content: integrity.to_string(),
                    mode: entry.unix_mode(),
                    sha256: format!("sha256={}", BASE64URL_NOPAD.encode(&sha256)),
                },
            );
        }
    }

    Ok(archive)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let archive = extract(cache.path(), &mut wheel).unwrap();

        // Rewind the file and try again.
        wheel.rewind().unwrap();
        let archive2 = extract(cache.path(), &mut wheel).unwrap();

        // The two archives should be the same
        assert_eq!(&archive, &archive2);

        // Assert that the cache contains the files
        insta::assert_debug_snapshot!(archive);
    }
}
