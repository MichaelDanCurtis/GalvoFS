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
    /// A workspace changed underneath a mutation, so the mutation was refused.
    ///
    /// The workspace is unchanged when this is returned. It is a distinct
    /// variant rather than an admission rejection because it is retryable and
    /// because callers map the two to different status codes.
    #[error("conflict: {0}")]
    Conflict(String),
}
