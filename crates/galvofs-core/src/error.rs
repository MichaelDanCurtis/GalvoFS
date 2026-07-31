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

impl GalvoFsError {
    /// A stable machine-readable slug for this failure.
    ///
    /// The `Display` text carries the human explanation and is free to change
    /// wording; a caller branching on it would be matching on prose. This is
    /// the part that is a contract.
    ///
    /// The vocabulary deliberately matches the one Femto already puts on the
    /// wire for these same failures, so a consumer does not have to learn two
    /// spellings for one condition depending on which layer answered.
    ///
    /// Adding a variant means adding a slug. The enum is `#[non_exhaustive]`,
    /// so downstream matches carry a wildcard and would silently absorb a new
    /// variant — this method has no wildcard for exactly that reason, making a
    /// forgotten slug a compile error here rather than a mislabelled error
    /// somewhere else.
    pub fn code(&self) -> &'static str {
        match self {
            Self::ManifestInvalid(_) => "manifest-invalid",
            Self::AdmissionRejected(_) => "admission-rejected",
            Self::RuntimeFailed(_) => "runtime-failed",
            Self::Expired(_) => "expired",
            Self::ArtifactMissing(_) => "artifact-missing",
            Self::Conflict(_) => "conflict",
        }
    }

    /// The human explanation, without the variant prefix `Display` adds.
    pub fn message(&self) -> &str {
        match self {
            Self::ManifestInvalid(message)
            | Self::AdmissionRejected(message)
            | Self::RuntimeFailed(message)
            | Self::Expired(message)
            | Self::ArtifactMissing(message)
            | Self::Conflict(message) => message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The slugs are a contract, so they are pinned literally.
    ///
    /// Asserting `error.code() == error.code()` or deriving the expectation
    /// from the same match would pass against any renaming. These are written
    /// out so a rename has to be a deliberate edit to this list.
    #[test]
    fn error_codes_are_stable() {
        let cases = [
            (
                GalvoFsError::ManifestInvalid("m".into()),
                "manifest-invalid",
            ),
            (
                GalvoFsError::AdmissionRejected("m".into()),
                "admission-rejected",
            ),
            (GalvoFsError::RuntimeFailed("m".into()), "runtime-failed"),
            (GalvoFsError::Expired("m".into()), "expired"),
            (
                GalvoFsError::ArtifactMissing("m".into()),
                "artifact-missing",
            ),
            (GalvoFsError::Conflict("m".into()), "conflict"),
        ];
        for (error, expected) in cases {
            assert_eq!(error.code(), expected);
            assert_eq!(error.message(), "m");
        }
    }
}
