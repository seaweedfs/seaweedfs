FROM openjdk:17-jdk-slim

# Install curl for debugging
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy Java files and dependencies
COPY *.java /app/
COPY *.jar /app/
WORKDIR /app

# Compile Java files (exclude Dockerfile)
RUN javac -cp ".:kafka-clients-8.0.0-ccs.jar:slf4j-api-1.7.36.jar:slf4j-simple-1.7.36.jar" SimpleSchemaRegistryTest.java MetadataTest.java

# Default command
CMD ["bash"]
