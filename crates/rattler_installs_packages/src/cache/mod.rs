//! This module contains code to interact with the cache.

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

#[derive(Debug, Error)]
pub enum CachedArchiveError {
    #[error("failed to read archive: {0}")]
    ArchiveRead(String),

    #[error("a generic IO error occurred")]
    IoError(#[from] std::io::Error),
}

/// Describes an archive who's contents have been extracted to the cache.
///
/// You can think of this as a tar file that has been extracted but instead of it being extracted
/// to the file system it's content has been extracted to a content-addressable store.
///
/// This type can be stored in the cache and later retrieved. Internally it uses a zero-copy buffer
/// which makes this extremely fast to serialize and deserialize.
pub struct CachedArchive {
    bytes: AlignedVec,
}

impl Debug for CachedArchive {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedArchive")
            .field("files", &self.files)
            .field("directories", &self.directories)
            .field("links", &self.links)
            .finish()
    }
}

impl PartialEq for CachedArchive {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl CachedArchive {
    /// Constructs a new instance from raw bytes.
    ///
    /// This will validate that the bytes are a valid archive.
    pub fn new(bytes: AlignedVec) -> Result<CachedArchive, CachedArchiveError> {
        let _ = rkyv::validation::validators::check_archived_root::<CachedArchiveData>(&bytes)
            .map_err(|e| CachedArchiveError::ArchiveRead(e.to_string()))?;
        Ok(Self { bytes })
    }

    /// Constructs a new instance by reading it from a reader.
    pub fn from_reader<R: Read>(mut rdr: R) -> Result<Self, CachedArchiveError> {
        let mut buf = AlignedVec::with_capacity(1024);
        buf.extend_from_reader(&mut rdr)?;
        Self::new(buf)
    }

    /// Write the underlying bytes of this archived value to the given writer.
    pub fn write<W: std::io::Write>(this: &Self, mut wtr: W) -> Result<(), std::io::Error> {
        wtr.write_all(&this.bytes)
    }

    /// Returns this instance as a byte representation which can be used to store the data in the
    /// cache.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Constructs a new instance from a [`CachedArchiveData`]
    pub fn from_unarchived(unarchived: &CachedArchiveData) -> Self {
        let bytes =
            rkyv::to_bytes::<_, 4096>(unarchived).expect("valid archive must serialize correctly");
        Self { bytes }
    }

    /// Deserializes this instance into [`CachedArchiveData`]. Note that this will require extract
    /// memory allocations.
    pub fn deserialize(this: &Self) -> CachedArchiveData {
        (**this)
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("valid archive must deserialize correctly")
    }
}

impl Deref for CachedArchive {
    type Target = <CachedArchiveData as rkyv::Archive>::Archived;

    fn deref(&self) -> &Self::Target {
        /// This is safe because we know that the bytes are a valid archive.
        unsafe {
            rkyv::archived_root::<CachedArchiveData>(&self.bytes)
        }
    }
}

/// Internal representation of a cached archive.
#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedArchiveData {
    /// All the files stored in the archive.
    pub files: HashMap<String, CachedArchiveEntryData>,

    /// Links stored in the archive.
    pub links: HashMap<String, String>,

    /// All the directories stored in the archive.
    pub directories: Vec<String>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
pub struct CachedArchiveEntryData {
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

/// Extracts the contents of a zip-archive to the cache returning a [`CachedArchiveData`].
///
/// The `archive_bytes` reader will be completely consumed by this function.
///
/// The [`CachedArchiveData`] will contain the details about all the files in the archive and how they
/// are stored in the cache.
///
/// All files in the cache will be deduplicated. Calling this function twice will yield the exact
/// same [`CachedArchiveData`].
pub fn extract_zip_archive(
    cache: &Path,
    archive_bytes: impl Read,
) -> Result<CachedArchive, ExtractZipArchiveError> {
    let mut archive = CachedArchiveData {
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
                CachedArchiveEntryData {
                    content: integrity.to_string(),
                    mode: entry.unix_mode(),
                },
            );
        }
    }

    dbg!("one");
    Ok(CachedArchive::from_unarchived(&archive))
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
        let archive = extract_zip_archive(cache.path(), &mut wheel).unwrap();

        // Rewind the file and try again.
        wheel.rewind().unwrap();
        let archive2 = extract_zip_archive(cache.path(), &mut wheel).unwrap();

        // The two archives should be the same
        assert_eq!(&archive, &archive2);

        // Assert that the cache contains the files
        insta::assert_debug_snapshot!(archive);
    }
}
