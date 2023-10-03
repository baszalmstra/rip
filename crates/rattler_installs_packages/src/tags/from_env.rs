use crate::tags::{WheelTag, WheelTags};
use crate::utils::VENDORED_PACKAGING_DIR;
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::process::ExitStatus;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FromPythonError {
    #[error("could not find python executable")]
    CouldNotFindPythonExecutable,

    #[error(transparent)]
    FailedToExecute(#[from] io::Error),

    #[error(transparent)]
    FailedToParse(#[from] serde_json::Error),

    #[error("execution failed with exit code {0}")]
    FailedToRun(ExitStatus),
}

impl WheelTags {
    /// Try to determine the platform tags by executing the python command and extracting the
    /// sys_tags. This will fail if the `packaging` module is not present in the python environment.
    /// In which case this method will return [`FromPythonError::PackagingModuleMissing`].
    pub async fn from_env() -> Result<Self, FromPythonError> {
        Self::from_python(Path::new("python")).await
    }

    /// Try to determine the platform tags by executing the python command and extracting the
    /// sys_tags. This will fail if the `packaging` module is not present in the python environment.
    /// In which case this method will return [`FromPythonError::PackagingModuleMissing`].
    pub async fn from_python(python: &Path) -> Result<Self, FromPythonError> {
        // Create a temporary directory to place our vendored packages in
        let vendored_dir = tempfile::tempdir()?;
        let packaging_target_dir = vendored_dir.path().join("packaging");
        tokio::fs::create_dir_all(&packaging_target_dir).await?;
        VENDORED_PACKAGING_DIR.extract(&packaging_target_dir)?;

        // Execute the python executable
        let output = match tokio::process::Command::new(python)
            .arg("-c")
            .arg(include_str!("platform_tags.py"))
            .env("PYTHONPATH", vendored_dir.path())
            .output()
            .await
        {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(FromPythonError::CouldNotFindPythonExecutable)
            }
            Err(e) => return Err(FromPythonError::FailedToExecute(e)),
            Ok(output) => output,
        };

        // Ensure that we have a valid success code
        if !output.status.success() {
            return Err(FromPythonError::FailedToRun(output.status));
        }

        // Convert the JSON
        let stdout = String::from_utf8_lossy(&output.stdout);
        let tags: Vec<(String, String, String)> = serde_json::from_str(stdout.trim())?;
        Ok(Self {
            tags: tags
                .into_iter()
                .map(|(interpreter, abi, platform)| WheelTag {
                    interpreter,
                    abi,
                    platform,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    pub async fn test_from_env() {
        match WheelTags::from_env().await {
            Err(FromPythonError::CouldNotFindPythonExecutable) => {
                // This is fine, the test machine does not include a python binary.
            }
            Err(e) => panic!("{e:?}"),
            Ok(tags) => {
                println!("Found the following platform tags on the current system:\n\n{tags:#?}")
            }
        }
    }
}
