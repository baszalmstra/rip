use crate::types::{NormalizedPackageName, ParsePackageNameError};
use pep440_rs::Version;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

/// The type of extension for an egg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EggExt {
    /// `.egg` extension
    Egg,

    /// `.egg-info` extension
    EggInfo,
}

impl EggExt {
    /// Returns the extension as a string (without the preceding `.` ).
    pub fn as_str(&self) -> &str {
        match self {
            EggExt::Egg => "egg-info",
            EggExt::EggInfo => "egg",
        }
    }
}

/// Represents an egg filename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EggFilename {
    /// The name of the package
    pub name: NormalizedPackageName,

    /// Optional version of the package
    pub version: Option<Version>,

    /// Optional required python version
    pub python_version: Option<String>,

    /// Optional required platform
    pub required_platform: Option<String>,

    /// The extension
    pub extension: EggExt,
}

impl Display for EggFilename {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name.as_str())?;
        if let Some(version) = &self.version {
            write!(f, "-{version}")?;
        }
        if let Some(python_version) = &self.python_version {
            write!(f, "-py{python_version}")?;
        }
        if let Some(required_platform) = &self.required_platform {
            write!(f, "-{required_platform}")?;
        }
        write!(f, ".{}", self.extension.as_str())
    }
}

/// An error that might occur when parsing an [`EggFilename`].
#[derive(Debug, Error)]
pub enum InvalidEggFilename {
    /// The extension of the filename is not recognized
    #[error("the extension is unrecognized, only '.egg' and '.egg-info' supported")]
    UnrecognizedExtension,

    /// Could not find a required package name in the filename
    #[error("the filename is missing a package name")]
    MissingName,

    /// Could not parse the package name as a valid normalized package name.
    #[error("{0}")]
    InvalidPackageName(ParsePackageNameError),

    /// The version is invalid
    #[error("invalid version: {0}")]
    InvalidVersion(String),

    /// The python version in the filename is invalid.
    #[error("invalid python version, must start with 'py'")]
    InvalidPythonVersion,
}

impl FromStr for EggFilename {
    type Err = InvalidEggFilename;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the extension
        let (extension, rest) = if let Some(rest) = s.strip_suffix(".egg") {
            (EggExt::Egg, rest)
        } else if let Some(rest) = s.strip_suffix(".egg-info") {
            (EggExt::EggInfo, rest)
        } else {
            return Err(InvalidEggFilename::UnrecognizedExtension);
        };

        // Split the rest of the string based on dashes.
        let mut split = rest.split('-');

        // Parse the name of the package
        let name = split
            .next()
            .ok_or(InvalidEggFilename::MissingName)
            .and_then(|s| {
                NormalizedPackageName::from_str(s).map_err(InvalidEggFilename::InvalidPackageName)
            })?;

        // Parse the optional version
        let version = split
            .next()
            .map(Version::from_str)
            .transpose()
            .map_err(InvalidEggFilename::InvalidVersion)?;

        // Parse the optional python version
        let python_version = match split.next().map(|s| s.strip_prefix("py")) {
            Some(Some(py_version)) => Some(py_version.to_string()),
            Some(None) => return Err(InvalidEggFilename::InvalidPythonVersion),
            None => None,
        };

        // Parse the required platform
        let required_platform = split.next().map(ToString::to_string);

        Ok(Self {
            name,
            version,
            python_version,
            required_platform,
            extension,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn test_egg_file_name() {
        let cases = [
            "simplejson-2.2.1-py3.9.egg-info",
            "pywin32-213-py2.6.egg",
            "simple.egg-info",
            "hunspell-0.3.3.egg-info",
            "pygobject-3.14.0-py2.7-linux-x86_64.egg-info",
        ];

        let conversion = cases
            .into_iter()
            .map(|s| (s, EggFilename::from_str(s)))
            .collect::<IndexMap<_, _>>();
        insta::assert_debug_snapshot!(conversion);
    }
}
