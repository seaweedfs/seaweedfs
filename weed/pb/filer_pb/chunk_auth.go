package filer_pb

// ChunkProofRefused starts the reason the filer gives when a scoped token's
// write names a chunk without a valid assignment token (chunk_auth); the file
// ids follow, comma-separated. It is distinct from a path refusal: the write
// cannot succeed by retrying, only by assigning and uploading the chunk again.
const ChunkProofRefused = "chunk proof refused:"
