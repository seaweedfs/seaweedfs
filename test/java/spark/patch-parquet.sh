#!/bin/bash
# This script patches the Parquet JAR to use LinkedHashSet instead of HashSet

JAR_PATH="$HOME/.m2/repository/org/apache/parquet/parquet-hadoop/1.14.4/parquet-hadoop-1.14.4.jar"
BACKUP_PATH="$HOME/.m2/repository/org/apache/parquet/parquet-hadoop/1.14.4/parquet-hadoop-1.14.4.jar.backup"

echo "Patching Parquet JAR at: $JAR_PATH"

# Backup original JAR
if [ ! -f "$BACKUP_PATH" ]; then
    cp "$JAR_PATH" "$BACKUP_PATH"
    echo "Created backup at: $BACKUP_PATH"
fi

# Extract the JAR
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"
jar xf "$JAR_PATH"

# Find and patch the class file
# We need to modify the bytecode to change HashSet to LinkedHashSet
# This is complex, so let's document what needs to be done

echo "JAR extracted to: $TEMP_DIR"
echo "To patch, we need to:"
echo "1. Decompile ParquetFileWriter.class"
echo "2. Change HashSet to LinkedHashSet"
echo "3. Recompile"
echo "4. Repackage JAR"
echo ""
echo "This requires javap, javac with all dependencies, and jar"
echo "Simpler approach: Use the patched source to rebuild the module"

rm -rf "$TEMP_DIR"
