use std::{fs, path::PathBuf};

use thiserror::Error;

use crate::{Environment, process};

#[derive(Debug)]
pub struct Backup {
    pub output: PathBuf,
}

#[derive(Debug)]
pub struct BackupReport {
    pub source: PathBuf,
    pub output: PathBuf,
    pub files: usize,
}

#[derive(Debug)]
pub struct Restore {
    pub input: PathBuf,
    pub force: bool,
}

#[derive(Debug)]
pub struct RestoreReport {
    pub input: PathBuf,
    pub output: PathBuf,
    pub files: usize,
}

#[derive(Debug)]
pub struct Uninstall {
    pub force: bool,
    pub dry: bool,
}

#[derive(Debug)]
pub struct UninstallReport {
    pub path: PathBuf,
    pub files: usize,
    pub dry: bool,
}

pub fn backup(environment: &impl Environment, request: &Backup) -> Result<BackupReport, Error> {
    let source = root(environment);
    if !source.exists() {
        return Err(Error::Absent(source));
    }
    if request.output.exists() {
        return Err(Error::Exists(request.output.clone()));
    }
    let files = copy(&source, &request.output)?;
    Ok(BackupReport {
        source,
        output: request.output.clone(),
        files,
    })
}

pub fn restore(environment: &impl Environment, request: &Restore) -> Result<RestoreReport, Error> {
    if !request.input.is_dir() {
        return Err(Error::Absent(request.input.clone()));
    }
    let output = root(environment);
    if output.exists() {
        if !request.force {
            return Err(Error::Exists(output));
        }
        fs::remove_dir_all(&output).map_err(|source| Error::Remove {
            path: output.clone(),
            source,
        })?;
    }
    let files = copy(&request.input, &output)?;
    Ok(RestoreReport {
        input: request.input.clone(),
        output,
        files,
    })
}

pub fn uninstall(
    environment: &impl Environment,
    request: &Uninstall,
) -> Result<UninstallReport, Error> {
    let path = root(environment);
    if !path.exists() {
        return Ok(UninstallReport {
            path,
            files: 0,
            dry: request.dry,
        });
    }
    if !request.force && !request.dry {
        return Err(Error::Force(path));
    }
    let files = count(&path)?;
    if !request.dry {
        fs::remove_dir_all(&path).map_err(|source| Error::Remove {
            path: path.clone(),
            source,
        })?;
    }
    Ok(UninstallReport {
        path,
        files,
        dry: request.dry,
    })
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(process::default_data, PathBuf::from)
}

fn count(path: &std::path::Path) -> Result<usize, Error> {
    let mut files = 0;
    for entry in fs::read_dir(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            files += count(&path)?;
        } else if path.is_file() {
            files += 1;
        }
    }
    Ok(files)
}

fn copy(source: &std::path::Path, output: &std::path::Path) -> Result<usize, Error> {
    fs::create_dir_all(output).map_err(|error| Error::Create {
        path: output.to_path_buf(),
        source: error,
    })?;
    let mut files = 0;
    for entry in fs::read_dir(source).map_err(|error| Error::Read {
        path: source.to_path_buf(),
        source: error,
    })? {
        let entry = entry.map_err(|error| Error::Read {
            path: source.to_path_buf(),
            source: error,
        })?;
        let path = entry.path();
        let target = output.join(entry.file_name());
        if path.is_dir() {
            files += copy(&path, &target)?;
        } else if path.is_file() {
            fs::copy(&path, &target).map_err(|error| Error::Copy {
                source: path,
                output: target,
                error,
            })?;
            files += 1;
        }
    }
    Ok(files)
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("backup source {0:?} does not exist")]
    Absent(PathBuf),
    #[error("backup destination {0:?} already exists")]
    Exists(PathBuf),
    #[error("refusing to uninstall data directory {0:?}; pass --force or use --dry-run")]
    Force(PathBuf),
    #[error("failed to create backup directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read backup directory {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to copy backup file from {source:?} to {output:?}")]
    Copy {
        source: PathBuf,
        output: PathBuf,
        #[source]
        error: std::io::Error,
    },
    #[error("failed to remove restore destination {path:?}")]
    Remove {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::DataEnv;
    use uuid::Uuid;

    #[test]
    fn uninstall_requires_force_and_removes_data_directory() {
        let root = std::env::temp_dir().join(format!("peren-uninstall-{}", Uuid::new_v4()));
        let data = root.join("data");
        fs::create_dir_all(data.join("tail")).unwrap();
        fs::write(data.join("tail/events.ndjson"), b"event").unwrap();

        assert!(matches!(
            uninstall(
                &DataEnv::new(data.clone()),
                &Uninstall {
                    force: false,
                    dry: false,
                },
            ),
            Err(Error::Force(path)) if path == data
        ));
        assert!(data.exists());

        let dry = uninstall(
            &DataEnv::new(data.clone()),
            &Uninstall {
                force: false,
                dry: true,
            },
        )
        .unwrap();
        assert_eq!(dry.files, 1);
        assert!(dry.dry);
        assert!(data.exists());

        let removed = uninstall(
            &DataEnv::new(data.clone()),
            &Uninstall {
                force: true,
                dry: false,
            },
        )
        .unwrap();
        assert_eq!(removed.files, 1);
        assert!(!data.exists());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn backup_and_restore_copy_data_directory() {
        let root = std::env::temp_dir().join(format!("peren-backup-{}", Uuid::new_v4()));
        let data = root.join("data");
        fs::create_dir_all(data.join("deploy")).unwrap();
        fs::write(data.join("deploy/deployments.json"), b"[]").unwrap();
        let archive = root.join("archive");

        let report = backup(
            &DataEnv::new(data.clone()),
            &Backup {
                output: archive.clone(),
            },
        )
        .unwrap();
        assert_eq!(report.files, 1);
        assert_eq!(
            fs::read(archive.join("deploy/deployments.json")).unwrap(),
            b"[]"
        );

        fs::remove_dir_all(&data).unwrap();
        let report = restore(
            &DataEnv::new(data.clone()),
            &Restore {
                input: archive,
                force: false,
            },
        )
        .unwrap();
        assert_eq!(report.files, 1);
        assert_eq!(
            fs::read(data.join("deploy/deployments.json")).unwrap(),
            b"[]"
        );

        fs::remove_dir_all(root).unwrap();
    }
}
