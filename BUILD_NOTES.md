# Build Notes for SeaweedFS Development

## Protobuf Generation

To regenerate protobuf Go files after modifying .proto files:

```bash
cd weed/pb
make
```

This will regenerate all the protobuf Go files with the latest changes from the .proto definitions.

## Other Important Build Commands

- Main build: `make`
- Clean build: `make clean && make`
- Tests: `make test`

---
*Generated: This file contains important build commands for development*
