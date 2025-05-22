#!/bin/bash

# Find all Go files containing glog.V calls
files=$(grep -l "glog.V" --include="*.go" -r .)

# Check if any files were found
if [ -z "$files" ]; then
    echo "No files found containing glog.V calls"
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
    # Replace glog.V(n).Info with log.V(n).Info for n=0-4
    for level in {0..4}; do
        # Replace Info calls
        sed -i '' "s/glog.V($level).Info/log.V($level).Info/g" "$file"
        # Replace Infof calls
        sed -i '' "s/glog.V($level).Infof/log.V($level).Infof/g" "$file"
    done
done

echo "Replacement complete!" 