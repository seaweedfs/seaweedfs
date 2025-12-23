# SeaweedFS Admin Component Makefile

# Variables
ADMIN_DIR := .
VIEW_DIR := $(ADMIN_DIR)/view
STATIC_DIR := $(ADMIN_DIR)/static
TEMPL_FILES := $(shell find $(VIEW_DIR) -name "*.templ")
TEMPL_GO_FILES := $(TEMPL_FILES:.templ=_templ.go)
GO_FILES := $(shell find $(ADMIN_DIR) -name "*.go" -not -name "*_templ.go")
BUILD_DIR := ../..
WEED_BINARY := $(BUILD_DIR)/weed

# Default target
.PHONY: all
all: build

# Install templ if not present
.PHONY: install-templ
install-templ:
	@which templ > /dev/null || (echo "Installing templ..." && go install github.com/a-h/templ/cmd/templ@latest)

# Generate templ files
.PHONY: generate
generate: install-templ
	@echo "Generating templ files..."
	@templ generate
	@echo "Generated: $(TEMPL_GO_FILES)"

# Clean generated files
.PHONY: clean-templ
clean-templ:
	@echo "Cleaning generated templ files..."
	@find $(VIEW_DIR) -name "*_templ.go" -delete
	@echo "Cleaned templ files"

# Watch for changes and regenerate
.PHONY: watch
watch: install-templ
	@echo "Watching for templ file changes..."
	@templ generate --watch

# Build the main weed binary with admin component
.PHONY: build
build: generate
	@echo "Building weed binary with admin component..."
	@cd $(BUILD_DIR) && go build -o weed ./weed
	@echo "Built: $(BUILD_DIR)/weed"

# Test the admin component
.PHONY: test
test: generate
	@echo "Running admin component tests..."
	@go test ./...

# Run the admin server via weed command
.PHONY: run
run: build
	@echo "Starting admin server via weed command..."
	@cd $(BUILD_DIR) && ./weed admin

# Development server with auto-reload
.PHONY: dev
dev: generate
	@echo "Starting development server with auto-reload..."
	@echo "Note: You'll need to manually restart the server when Go files change"
	@cd $(BUILD_DIR) && ./weed admin -port=23647 &
	@$(MAKE) watch

# Lint the code
.PHONY: lint
lint:
	@echo "Linting admin component..."
	@golangci-lint run ./...

# Format the code
.PHONY: fmt
fmt:
	@echo "Formatting Go code..."
	@go fmt ./...
	@echo "Formatting templ files..."
	@templ fmt $(VIEW_DIR)

# Validate static files exist
.PHONY: validate-static
validate-static:
	@echo "Validating static files..."
	@test -f $(STATIC_DIR)/css/admin.css || (echo "Missing: admin.css" && exit 1)
	@test -f $(STATIC_DIR)/js/admin.js || (echo "Missing: admin.js" && exit 1)
	@echo "Static files validated"

# Check dependencies
.PHONY: deps
deps:
	@echo "Checking dependencies..."
	@go mod tidy
	@go mod verify

# Clean all build artifacts
.PHONY: clean
clean: clean-templ
	@echo "Cleaning build artifacts..."
	@rm -f $(BUILD_DIR)/weed 2>/dev/null || true
	@echo "Cleaned build artifacts"

# Install dependencies
.PHONY: install-deps
install-deps:
	@echo "Installing Go dependencies..."
	@go mod download
	@$(MAKE) install-templ

# Production build
.PHONY: build-prod
build-prod: clean generate validate-static
	@echo "Building production binary..."
	@cd $(BUILD_DIR) && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o weed-linux-amd64 ./weed
	@echo "Built production binary: $(BUILD_DIR)/weed-linux-amd64"

# Docker build (if needed)
.PHONY: docker-build
docker-build: generate
	@echo "Building Docker image with admin component..."
	@cd $(BUILD_DIR) && docker build -t seaweedfs/seaweedfs:latest .

# Help target
.PHONY: help
help:
	@echo "SeaweedFS Admin Component Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  all              - Build the weed binary with admin component (default)"
	@echo "  generate         - Generate templ files from templates"
	@echo "  build            - Build weed binary with admin component"
	@echo "  build-prod       - Build production binary"
	@echo "  run              - Run admin server via weed command"
	@echo "  dev              - Start development server with template watching"
	@echo "  test             - Run tests"
	@echo "  watch            - Watch for template changes and regenerate"
	@echo "  clean            - Clean all build artifacts"
	@echo "  clean-templ      - Clean generated template files"
	@echo "  fmt              - Format Go and templ code"
	@echo "  lint             - Lint the code"
	@echo "  deps             - Check and tidy dependencies"
	@echo "  install-deps     - Install all dependencies"
	@echo "  install-templ    - Install templ compiler"
	@echo "  validate-static  - Validate static files exist"
	@echo "  docker-build     - Build Docker image"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make generate    # Generate templates"
	@echo "  make build       # Build weed binary"
	@echo "  make run         # Start admin server"
	@echo "  make dev         # Development mode with auto-reload"

# Make sure generated files are up to date before building
$(WEED_BINARY): $(TEMPL_GO_FILES) $(GO_FILES)
	@$(MAKE) build

# Auto-generate templ files when .templ files change
%_templ.go: %.templ
	@echo "Regenerating $@ from $<"
	@templ generate

.PHONY: $(TEMPL_GO_FILES) 
