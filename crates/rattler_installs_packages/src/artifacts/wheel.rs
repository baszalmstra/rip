use crate::types::HasArtifactName;
use crate::{
    types::{
        ArtifactFromBytes, NormalizedPackageName, PackageName, RFC822ish, WheelCoreMetaDataError,
        WheelCoreMetadata, WheelFilename,
    },
    utils::ReadAndSeek,
};
use async_http_range_reader::AsyncHttpRangeReader;
use async_zip::base::read::seek::ZipFileReader;
use fs_err as fs;
use miette::IntoDiagnostic;
use parking_lot::Mutex;
use pep440_rs::Version;
use std::{
    borrow::Cow,
    ffi::OsStr,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tokio_util::compat::TokioAsyncReadCompatExt;
use zip::{result::ZipError, ZipArchive};

/// Different representations of a wheel that is stored locally on disk.
pub enum LocalWheel {
    /// A `.whl` file on disk.
    ArchivedWheel(ArchivedWheel),
}

/// A wheel file (`.whl`) in its archived form that is stored somewhere on disk.
pub struct ArchivedWheel {
    /// Name of wheel
    pub name: WheelFilename,

    pub(crate) archive: Mutex<ZipArchive<Box<dyn ReadAndSeek + Send>>>,
}

impl HasArtifactName for ArchivedWheel {
    type Name = WheelFilename;

    fn name(&self) -> &Self::Name {
        &self.name
    }
}

impl HasArtifactName for LocalWheel {
    type Name = WheelFilename;

    fn name(&self) -> &Self::Name {
        match self {
            LocalWheel::ArchivedWheel(whl) => whl.name(),
        }
    }
}

impl ArtifactFromBytes for ArchivedWheel {
    fn from_bytes(name: Self::Name, bytes: Box<dyn ReadAndSeek + Send>) -> miette::Result<Self> {
        Ok(Self {
            name,
            archive: Mutex::new(ZipArchive::new(bytes).into_diagnostic()?),
        })
    }
}

impl ArtifactFromBytes for LocalWheel {
    fn from_bytes(name: Self::Name, bytes: Box<dyn ReadAndSeek + Send>) -> miette::Result<Self> {
        // It is assumed that when constructing a LocalWheel from bytes that the bytes refer to a
        // zipped archive.
        ArchivedWheel::from_bytes(name, bytes).map(LocalWheel::ArchivedWheel)
    }
}

impl ArchivedWheel {
    /// Open a wheel by reading a `.whl` file on disk.
    pub fn from_path(
        path: &Path,
        normalized_package_name: &NormalizedPackageName,
    ) -> miette::Result<Self> {
        let file_name = path
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| miette::miette!("path does not contain a filename"))?;
        let wheel_name =
            WheelFilename::from_filename(file_name, normalized_package_name).into_diagnostic()?;
        let file = fs::File::open(path).into_diagnostic()?;
        Self::from_bytes(wheel_name, Box::new(file))
    }

    /// Create a wheel from URL and content.
    pub fn from_url_and_bytes(
        url: &str,
        normalized_package_name: &NormalizedPackageName,
        bytes: Box<dyn ReadAndSeek + Send>,
    ) -> miette::Result<Self> {
        let url_path = PathBuf::from_str(url).into_diagnostic()?;
        let file_name = url_path
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| {
                miette::miette!("path {:?} does not contain a wheel filename", url_path)
            })?;
        let wheel_filename =
            WheelFilename::from_filename(file_name, normalized_package_name).into_diagnostic()?;

        Self::from_bytes(wheel_filename.clone(), Box::new(bytes))
    }

    async fn get_lazy_vitals(
        name: &WheelFilename,
        stream: &mut AsyncHttpRangeReader,
    ) -> Result<(Vec<u8>, WheelCoreMetadata), WheelVitalsError> {
        // Make sure we have the back part of the stream.
        // Best guess for the central directory size inside the zip
        const CENTRAL_DIRECTORY_SIZE: u64 = 16384;
        // Because the zip index is at the back
        stream
            .prefetch(stream.len().saturating_sub(CENTRAL_DIRECTORY_SIZE)..stream.len())
            .await;

        // Construct a zip reader to uses the stream.
        let mut reader = ZipFileReader::new(stream.compat())
            .await
            .map_err(|err| WheelVitalsError::from_async_zip("/".into(), err))?;

        // Collect all top-level filenames
        let file_names = reader
            .file()
            .entries()
            .iter()
            .filter_map(|e| e.filename().as_str().ok());

        // Determine the name of the dist-info directory
        let dist_info_prefix = find_dist_info(&name, file_names)?.to_owned();

        let metadata_path = format!("{dist_info_prefix}.dist-info/METADATA");
        let (metadata_idx, metadata_entry) = reader
            .file()
            .entries()
            .iter()
            .enumerate()
            .find(|(_, p)| p.filename().as_str().ok() == Some(metadata_path.as_str()))
            .ok_or(WheelVitalsError::MetadataMissing)?;

        // Get the size of the entry plus the header + size of the filename. We should also actually
        // include bytes for the extra fields but we don't have that information.
        let offset = metadata_entry.header_offset();
        let size = metadata_entry.compressed_size()
            + 30 // Header size in bytes
            + metadata_entry.filename().as_bytes().len() as u64;

        // The zip archive uses as BufReader which reads in chunks of 8192. To ensure we prefetch
        // enough data we round the size up to the nearest multiple of the buffer size.
        let buffer_size = 8192;
        let size = ((size + buffer_size - 1) / buffer_size) * buffer_size;

        // Fetch the bytes from the zip archive that contain the requested file.
        reader
            .inner_mut()
            .get_mut()
            .prefetch(offset..offset + size)
            .await;

        // Read the contents of the metadata.json file
        let mut contents = Vec::new();
        reader
            .reader_with_entry(metadata_idx)
            .await
            .map_err(|e| WheelVitalsError::from_async_zip(metadata_path.clone(), e))?
            .read_to_end_checked(&mut contents)
            .await
            .map_err(|e| WheelVitalsError::from_async_zip(metadata_path, e))?;

        // Parse the wheel data
        let metadata = WheelCoreMetadata::try_from(contents.as_slice())?;

        let stream = reader.into_inner().into_inner();
        let ranges = stream.requested_ranges().await;
        let total_bytes_fetched: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        tracing::debug!(
            "fetched {} ranges, total of {} bytes, total file length {} ({}%)",
            ranges.len(),
            total_bytes_fetched,
            stream.len(),
            (total_bytes_fetched as f64 / stream.len() as f64 * 100000.0).round() / 100.0
        );

        Ok((contents, metadata))
    }

    /// Get the metadata from the wheel archive
    pub fn metadata(&self) -> Result<(Vec<u8>, WheelCoreMetadata), WheelVitalsError> {
        let mut archive = self.archive.lock();

        // Determine the name of the dist-info directory
        let dist_info_prefix = find_dist_info(&self.name, archive.file_names())?.to_owned();

        // Read the METADATA file
        let metadata_path = format!("{dist_info_prefix}.dist-info/METADATA");
        let metadata_blob = read_entry_to_end(&mut archive, &metadata_path)?;
        let metadata = WheelCoreMetadata::try_from(metadata_blob.as_slice())?;

        // Verify the contents of the METADATA
        if metadata.name != self.name.distribution {
            return Err(WheelCoreMetaDataError::FailedToParse(format!(
                "name mismatch between {dist_info_prefix}.dist-info/METADATA and filename ({} != {}",
                metadata.name.as_source_str(),
                self.name.distribution.as_source_str()
            ))
            .into());
        }
        if metadata.version != self.name.version {
            return Err(WheelCoreMetaDataError::FailedToParse(format!(
                "version mismatch between {dist_info_prefix}.dist-info/METADATA and filename ({} != {})",
                metadata.version, self.name.version
            ))
            .into());
        }

        Ok((metadata_blob, metadata))
    }

    /// Read metadata from bytes-stream
    pub async fn read_metadata_bytes(
        name: &WheelFilename,
        stream: &mut AsyncHttpRangeReader,
    ) -> miette::Result<(Vec<u8>, WheelCoreMetadata)> {
        Self::get_lazy_vitals(name, stream).await.into_diagnostic()
    }
}

#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum WheelVitalsError {
    #[error(".dist-info/ missing")]
    DistInfoMissing,

    #[error(".dist-info/WHEEL missing")]
    WheelMissing,

    #[error(".dist-info/METADATA missing")]
    MetadataMissing,

    #[error("found multiple {0} directories in wheel")]
    MultipleSpecialDirs(Cow<'static, str>),

    #[error("failed to parse WHEEL file")]
    FailedToParseWheel(#[source] <RFC822ish as FromStr>::Err),

    #[error("unsupported WHEEL version {0}")]
    UnsupportedWheelVersion(String),

    #[error("invalid METADATA")]
    InvalidMetadata(#[from] WheelCoreMetaDataError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Failed to read the wheel file {0}")]
    ZipError(String, #[source] ZipError),

    #[error("Failed to read the wheel file {0}: {1}")]
    AsyncZipError(String, #[source] async_zip::error::ZipError),

    #[error("missing key from WHEEL '{0}'")]
    MissingKeyInWheel(String),
}

impl WheelVitalsError {
    pub(crate) fn from_zip(file: String, err: ZipError) -> Self {
        match err {
            ZipError::Io(err) => WheelVitalsError::IoError(err),
            ZipError::FileNotFound => {
                if file.ends_with("WHEEL") {
                    WheelVitalsError::WheelMissing
                } else if file.ends_with("METADATA") {
                    WheelVitalsError::MetadataMissing
                } else {
                    WheelVitalsError::ZipError(file, err)
                }
            }
            _ => WheelVitalsError::ZipError(file, err),
        }
    }

    pub(crate) fn from_async_zip(file: String, err: async_zip::error::ZipError) -> Self {
        match err {
            async_zip::error::ZipError::UpstreamReadError(err) => WheelVitalsError::IoError(err),
            _ => WheelVitalsError::AsyncZipError(file, err),
        }
    }
}

/// Helper method to read a particular file from a zip archive.
fn read_entry_to_end<R: ReadAndSeek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<Vec<u8>, WheelVitalsError> {
    let mut bytes = Vec::new();
    archive
        .by_name(name)
        .map_err(|err| WheelVitalsError::from_zip(name.to_string(), err))?
        .read_to_end(&mut bytes)?;

    Ok(bytes)
}

/// Locates the `.dist-info` directory in a list of files.
pub(crate) fn find_dist_info<'a>(
    wheel_name: &WheelFilename,
    files: impl IntoIterator<Item = &'a str>,
) -> Result<&'a str, WheelVitalsError> {
    let mut dist_infos = files.into_iter().filter_map(|path| {
        let (dir_name, rest) = path.split_once(['/', '\\'])?;
        let dir_stem = dir_name.strip_suffix(".dist-info")?;
        let (name, version) = dir_stem.rsplit_once('-')?;
        if PackageName::from_str(name).ok()? == wheel_name.distribution
            && Version::from_str(version).ok()? == wheel_name.version
            && rest == "METADATA"
        {
            Some(dir_stem)
        } else {
            None
        }
    });

    match (dist_infos.next(), dist_infos.next()) {
        (Some(path), None) => Ok(path),
        (Some(_), Some(_)) => Err(WheelVitalsError::MultipleSpecialDirs("dist-info".into())),
        _ => Err(WheelVitalsError::DistInfoMissing),
    }
}
