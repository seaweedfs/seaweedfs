FROM openjdk:17-jdk-slim

# Install curl for downloading JARs
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the Java source and build script
COPY SchemaRegistrySimulator.java .
COPY build.sh .

# Make build script executable
RUN chmod +x build.sh

# Download dependencies and compile
RUN ./build.sh

# Default command
CMD ["java", "-cp", ".:kafka-clients-8.0.0-ccs.jar:slf4j-api-1.7.36.jar:slf4j-simple-1.7.36.jar", "SchemaRegistrySimulator"]


