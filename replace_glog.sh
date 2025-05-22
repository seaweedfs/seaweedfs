#!/bin/bash

# Find all Go files containing glog calls
files=$(grep -l "glog\." --include="*.go" -r .)

# Check if any files were found
if [ -z "$files" ]; then
    echo "No files found containing glog calls"
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
    # Replace all glog function calls with log
    sed -i '' 's/glog\./log\./g' "$file"
done

echo "Replacement complete!" 