# Needle Layout Format

This document describes the binary layout of the Needle structure as used in SeaweedFS storage, for all supported versions (v1, v2, v3).

A Needle represents a file or data blob stored in a volume file. The layout determines how the Needle is serialized to disk for efficient storage and retrieval.

---

## Common Field Sizes

| Field         | Size (bytes) |
|---------------|--------------|
| Cookie        | 4            |
| NeedleId      | 8            |
| Size          | 4            |
| DataSize      | 4            |
| Flags         | 1            |
| NameSize      | 1            |
| MimeSize      | 1            |
| LastModified  | 5            |
| Ttl           | 2            |
| PairsSize     | 2            |
| Checksum      | 4            |
| Timestamp     | 8            |

---

## Needle Layouts by Version

### Version 1

| Offset   | Field    | Size (bytes) | Description                                   |
|----------|----------|--------------|-----------------------------------------------|
| 0        | Cookie   | 4            | Random number to mitigate brute force lookups |
| 4        | Id       | 8            | Needle ID                                     |
| 12       | Size     | 4            | Length of Data                                |
| 16       | Data     | N            | File data (N = Size)                          |
| 16+N     | Checksum | 4            | CRC32 of Data                                 |
| 20+N     | Padding  | 0-7          | To align to 8 bytes                           |

### Version 2

| Offset   | Field        | Size (bytes) | Description                                   |
|----------|-------------|--------------|-----------------------------------------------|
| 0        | Cookie      | 4            | Random number                                 |
| 4        | Id          | 8            | Needle ID                                     |
| 12       | Size        | 4            | Total size of the following fields            |
| 16       | DataSize    | 4            | Length of Data (N)                            |
| 20       | Data        | N            | File data                                     |
| 20+N     | Flags       | 1            | Bit flags                                     |
| 21+N     | NameSize    | 1 (opt)      | Optional, if present                          |
| 22+N     | Name        | M (opt)      | Optional, if present (M = NameSize)           |
| ...      | MimeSize    | 1 (opt)      | Optional, if present                          |
| ...      | Mime        | K (opt)      | Optional, if present (K = MimeSize)           |
| ...      | LastModified| 5 (opt)      | Optional, if present                          |
| ...      | Ttl         | 2 (opt)      | Optional, if present                          |
| ...      | PairsSize   | 2 (opt)      | Optional, if present                          |
| ...      | Pairs       | P (opt)      | Optional, if present (P = PairsSize)          |
| ...      | Checksum    | 4            | CRC32                                         |
| ...      | Padding     | 0-7          | To align to 8 bytes                           |

### Version 3

| Offset   | Field        | Size (bytes) | Description                                   |
|----------|-------------|--------------|-----------------------------------------------|
| 0        | Cookie      | 4            | Random number                                 |
| 4        | Id          | 8            | Needle ID                                     |
| 12       | Size        | 4            | Total size of the following fields            |
| 16       | DataSize    | 4            | Length of Data (N)                            |
| 20       | Data        | N            | File data                                     |
| 20+N     | Flags       | 1            | Bit flags                                     |
| 21+N     | NameSize    | 1 (opt)      | Optional, if present                          |
| 22+N     | Name        | M (opt)      | Optional, if present (M = NameSize)           |
| ...      | MimeSize    | 1 (opt)      | Optional, if present                          |
| ...      | Mime        | K (opt)      | Optional, if present (K = MimeSize)           |
| ...      | LastModified| 5 (opt)      | Optional, if present                          |
| ...      | Ttl         | 2 (opt)      | Optional, if present                          |
| ...      | PairsSize   | 2 (opt)      | Optional, if present                          |
| ...      | Pairs       | P (opt)      | Optional, if present (P = PairsSize)          |
| ...      | Checksum    | 4            | CRC32                                         |
| ...      | Timestamp   | 8            | Append time in nanoseconds                    |
| ...      | Padding     | 0-7          | To align to 8 bytes                           |

- Offsets marked with `...` depend on the presence and size of previous optional fields.
- Fields marked `(opt)` are optional and only present if the corresponding size or flag is non-zero.
- N = DataSize, M = NameSize, K = MimeSize, P = PairsSize.

---

## Field Explanations

- **Cookie**: 4 bytes, random value for security.
- **Id**: 8 bytes, unique identifier for the Needle.
- **Size**: 4 bytes, total size of the Needle data section (not including header, checksum, timestamp, or padding).
- **DataSize**: 4 bytes, length of the Data field.
- **Data**: File data (variable length).
- **Flags**: 1 byte, bit flags for Needle properties.
- **NameSize/Name**: 1 byte + variable, optional file name.
- **MimeSize/Mime**: 1 byte + variable, optional MIME type.
- **LastModified**: 5 bytes, optional last modified timestamp.
- **Ttl**: 2 bytes, optional time-to-live.
- **PairsSize/Pairs**: 2 bytes + variable, optional key-value pairs.
- **Checksum**: 4 bytes, CRC32 checksum of the Needle data.
- **Timestamp**: 8 bytes, append time (only in v3).
- **Padding**: 0-7 bytes, to align the total Needle size to 8 bytes.

---

## Version Comparison Table

| Field         | v1 | v2 | v3 |
|---------------|----|----|----|
| Cookie        | ✔  | ✔  | ✔  |
| Id            | ✔  | ✔  | ✔  |
| Size          | ✔  | ✔  | ✔  |
| DataSize      |    | ✔  | ✔  |
| Data          | ✔  | ✔  | ✔  |
| Flags         |    | ✔  | ✔  |
| NameSize/Name |    | ✔  | ✔  |
| MimeSize/Mime |    | ✔  | ✔  |
| LastModified  |    | ✔  | ✔  |
| Ttl           |    | ✔  | ✔  |
| PairsSize/Pairs|   | ✔  | ✔  |
| Checksum      | ✔  | ✔  | ✔  |
| Timestamp     |    |    | ✔  |
| Padding       | ✔  | ✔  | ✔  |

---

## Flags Field Details

The `Flags` field (present in v2 and v3) is a bitmask that encodes several boolean properties of the Needle. Each bit has a specific meaning:

| Bit Value | Name                  | Meaning                                                      |
|-----------|-----------------------|--------------------------------------------------------------|
| 0x01      | FlagIsCompressed      | Data is compressed (isCompressed)                            |
| 0x02      | FlagHasName           | Name field is present (NameSize/Name)                        |
| 0x04      | FlagHasMime           | Mime field is present (MimeSize/Mime)                        |
| 0x08      | FlagHasLastModifiedDate| LastModified field is present                                |
| 0x10      | FlagHasTtl            | Ttl field is present                                         |
| 0x20      | FlagHasPairs          | Pairs field is present (PairsSize/Pairs)                     |
| 0x80      | FlagIsChunkManifest   | Data is a chunk manifest (for large files)                   |

- If a flag is set, the corresponding field(s) will appear in the Needle layout at the appropriate position.
- The `Flags` field is always present in v2 and v3, immediately after the Data field.

## Optional Fields

- Fields marked as optional in the layout tables are only present if the corresponding flag in the `Flags` field is set (except for Name/Mime/Pairs, which also depend on their size fields being non-zero).
- The order of optional fields is fixed and matches the order of their flags.

## Special Notes

- **isCompressed**: If set, the Data field is compressed (typically using gzip). This is indicated by the lowest bit (0x01) in the Flags byte.
- **isChunkManifest**: If set, the Data field contains a manifest describing chunks of a large file, not raw file data.
- **All multi-byte fields** are stored in big-endian order.
- **Padding** is always added at the end to align the total Needle size to 8 bytes.
- **N = DataSize, M = NameSize, K = MimeSize, P = PairsSize** in the layout tables above.

For more details, see the implementation in the corresponding Go files in this directory. 