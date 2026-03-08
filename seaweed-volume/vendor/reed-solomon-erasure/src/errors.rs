use core::fmt::Formatter;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Error {
    TooFewShards,
    TooManyShards,
    TooFewDataShards,
    TooManyDataShards,
    TooFewParityShards,
    TooManyParityShards,
    TooFewBufferShards,
    TooManyBufferShards,
    IncorrectShardSize,
    TooFewShardsPresent,
    EmptyShard,
    InvalidShardFlags,
    InvalidIndex,
}

impl Error {
    fn to_string(&self) -> &str {
        match *self {
            Error::TooFewShards=> "The number of provided shards is smaller than the one in codec",
            Error::TooManyShards => "The number of provided shards is greater than the one in codec",
            Error::TooFewDataShards => "The number of provided data shards is smaller than the one in codec",
            Error::TooManyDataShards => "The number of provided data shards is greater than the one in codec",
            Error::TooFewParityShards => "The number of provided parity shards is smaller than the one in codec",
            Error::TooManyParityShards => "The number of provided parity shards is greater than the one in codec",
            Error::TooFewBufferShards => "The number of provided buffer shards is smaller than the number of parity shards in codec",
            Error::TooManyBufferShards => "The number of provided buffer shards is greater than the number of parity shards in codec",
            Error::IncorrectShardSize => "At least one of the provided shards is not of the correct size",
            Error::TooFewShardsPresent => "The number of shards present is smaller than number of parity shards, cannot reconstruct missing shards",
            Error::EmptyShard => "The first shard provided is of zero length",
            Error::InvalidShardFlags => "The number of flags does not match the total number of shards",
            Error::InvalidIndex => "The data shard index provided is greater or equal to the number of data shards in codec",
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), core::fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {
    fn description(&self) -> &str {
        self.to_string()
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum SBSError {
    TooManyCalls,
    LeftoverShards,
    RSError(Error),
}

impl SBSError {
    fn to_string(&self) -> &str {
        match *self {
            SBSError::TooManyCalls => "Too many calls",
            SBSError::LeftoverShards => "Leftover shards",
            SBSError::RSError(ref e) => e.to_string(),
        }
    }
}

impl core::fmt::Display for SBSError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), core::fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SBSError {
    fn description(&self) -> &str {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::Error;
    use crate::errors::SBSError;

    #[test]
    fn test_error_to_string_is_okay() {
        assert_eq!(
            Error::TooFewShards.to_string(),
            "The number of provided shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyShards.to_string(),
            "The number of provided shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewDataShards.to_string(),
            "The number of provided data shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyDataShards.to_string(),
            "The number of provided data shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewParityShards.to_string(),
            "The number of provided parity shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyParityShards.to_string(),
            "The number of provided parity shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewBufferShards.to_string(),
            "The number of provided buffer shards is smaller than the number of parity shards in codec"
        );
        assert_eq!(
            Error::TooManyBufferShards.to_string(),
            "The number of provided buffer shards is greater than the number of parity shards in codec"
        );
        assert_eq!(
            Error::IncorrectShardSize.to_string(),
            "At least one of the provided shards is not of the correct size"
        );
        assert_eq!(Error::TooFewShardsPresent.to_string(), "The number of shards present is smaller than number of parity shards, cannot reconstruct missing shards");
        assert_eq!(
            Error::EmptyShard.to_string(),
            "The first shard provided is of zero length"
        );
        assert_eq!(
            Error::InvalidShardFlags.to_string(),
            "The number of flags does not match the total number of shards"
        );
        assert_eq!(
            Error::InvalidIndex.to_string(),
            "The data shard index provided is greater or equal to the number of data shards in codec"
        );
    }

    #[test]
    fn test_sbserror_to_string_is_okay() {
        assert_eq!(SBSError::TooManyCalls.to_string(), "Too many calls");
        assert_eq!(SBSError::LeftoverShards.to_string(), "Leftover shards");
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_error_display_does_not_panic() {
        println!("{}", Error::TooFewShards);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_sbserror_display_does_not_panic() {
        println!("{}", SBSError::TooManyCalls);
    }
}
