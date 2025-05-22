#!/bin/bash

# Find all Go files containing the old import path
files=$(grep -l "github.com/seaweedfs/seaweedfs/weed/glog" --include="*.go" -r .)

# Check if any files were found
if [ -z "$files" ]; then
    echo "No files found containing the old import path"
    exit 0
fi

# Print the files that will be modified
echo "The following files will be modified:"
echo "$files"
echo

# Ask for confirmation
read -p "Do you want to proceed with the replacement? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled"
    exit 1
fi

# Make the replacements
for file in $files; do
    echo "Processing $file"
    # Use sed to replace the import path
    sed -i '' 's|github.com/seaweedfs/seaweedfs/weed/glog|github.com/seaweedfs/seaweedfs/weed/util/log|g' "$file"
done

echo "Replacement complete!" 