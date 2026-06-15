use thiserror::Error;

pub type GalvoFsResult<T> = Result<T, GalvoFsError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum GalvoFsError {
    #[error("manifest invalid: {0}")]
    ManifestInvalid(String),
    #[error("admission rejected: {0}")]
    AdmissionRejected(String),
    #[error("runtime failed: {0}")]
    RuntimeFailed(String),
    #[error("expired: {0}")]
    Expired(String),
    #[error("artifact missing: {0}")]
    ArtifactMissing(String),
}
