#!/bin/bash

# Find all Go files containing log.V calls
files=$(grep -l "log.V" --include="*.go" -r .)

if [ -z "$files" ]; then
    echo "No files found containing log.V calls"
    exit 0
fi

# Create a temporary file for sed operations
temp_file=$(mktemp)

# Process each file
for file in $files; do
    echo "Processing $file"
    
    # First, replace log.V(-1) with a temporary placeholder
    sed 's/log\.V(-1)/__TEMP_NEG_ONE__/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    
    # Replace log.V(4) with log.V(-1)
    sed 's/log\.V(4)/log.V(-1)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    
    # Replace the temporary placeholder with log.V(4)
    sed 's/__TEMP_NEG_ONE__/log.V(4)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    
    # Swap log.V(0) and log.V(3)
    sed 's/log\.V(0)/__TEMP_ZERO__/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    sed 's/log\.V(3)/log.V(0)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    sed 's/__TEMP_ZERO__/log.V(3)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    
    # Swap log.V(1) and log.V(2)
    sed 's/log\.V(1)/__TEMP_ONE__/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    sed 's/log\.V(2)/log.V(1)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    sed 's/__TEMP_ONE__/log.V(2)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
    
    # Replace any other log.V(n) with log.V(-1)
    sed -E 's/log\.V\([5-9][0-9]*\)/log.V(-1)/g' "$file" > "$temp_file"
    mv "$temp_file" "$file"
done

# Clean up
rm -f "$temp_file"

echo "Log level swapping completed!" 