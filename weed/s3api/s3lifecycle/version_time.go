package s3lifecycle


// versionIdFormatThreshold distinguishes old vs new format version IDs.
// New format (inverted timestamps) produces values above this threshold;
// old format (raw timestamps) produces values below it.
const versionIdFormatThreshold = 0x4000000000000000

