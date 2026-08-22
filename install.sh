#!/bin/bash
#
# SeaweedFS Installer
# Downloads Go and/or Rust binaries from GitHub releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/seaweedfs/seaweedfs/master/install.sh | bash
#   curl -fsSL ... | bash -s -- --component volume-rust --large-disk
#   curl -fsSL ... | bash -s -- --version 4.34 --dir /usr/local/bin
#
# Options:
#   --component COMP   Which binary to install: weed, volume-rust, worker-rust, all (default: weed)
#   --version VER      Release version tag (default: latest)
#   --large-disk       Use large disk variant (5-byte offset, 8TB max volume)
#   --dir DIR          Installation directory (default: /usr/local/bin)
#   --help             Show this help message

set -euo pipefail

REPO="seaweedfs/seaweedfs"
COMPONENT="weed"
VERSION=""
LARGE_DISK=false
INSTALL_DIR="/usr/local/bin"
WORKER_INSTALLED=false

# Colors (if terminal supports them)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' NC=''
fi

info()  { echo -e "${BLUE}[info]${NC} $*"; }
ok()    { echo -e "${GREEN}[ok]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

usage() {
    sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \?//'
    exit 0
}

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --component)  COMPONENT="$2"; shift 2 ;;
        --version)    VERSION="$2"; shift 2 ;;
        --large-disk) LARGE_DISK=true; shift ;;
        --dir)        INSTALL_DIR="$2"; shift 2 ;;
        --help|-h)    usage ;;
        *)            error "Unknown option: $1. Use --help for usage." ;;
    esac
done

# Detect OS and architecture
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux*)   os="linux" ;;
        Darwin*)  os="darwin" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        FreeBSD*) os="freebsd" ;;
        *)        error "Unsupported OS: $(uname -s)" ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)   arch="amd64" ;;
        aarch64|arm64)  arch="arm64" ;;
        armv7l|armv6l)  arch="arm" ;;
        *)              error "Unsupported architecture: $(uname -m)" ;;
    esac

    echo "${os}" "${arch}"
}

# Get latest release tag from GitHub API
get_latest_version() {
    local url="https://api.github.com/repos/${REPO}/releases/latest"
    if command -v curl &>/dev/null; then
        curl -fsSL "$url" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/'
    elif command -v wget &>/dev/null; then
        wget -qO- "$url" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/'
    else
        error "Neither curl nor wget found. Please install one."
    fi
}

# Download a file
download() {
    local url="$1" dest="$2"
    info "Downloading ${url}"
    if command -v curl &>/dev/null; then
        curl -fsSL -o "$dest" "$url"
    elif command -v wget &>/dev/null; then
        wget -qO "$dest" "$url"
    fi
}

# Build Go weed binary asset name
go_asset_name() {
    local os="$1" arch="$2"
    local suffix="${os}_${arch}"
    if [ "$LARGE_DISK" = true ]; then
        suffix="${suffix}_large_disk"
    fi
    echo "${suffix}.tar.gz"
}

# Build Rust volume server asset name
rust_asset_name() {
    local os="$1" arch="$2"
    local prefix="weed-volume"
    if [ "$LARGE_DISK" = true ]; then
        prefix="weed-volume_large_disk"
    else
        prefix="weed-volume"
    fi
    local suffix="${os}_${arch}"
    if [ "$os" = "windows" ]; then
        echo "${prefix}_${suffix}.zip"
    else
        echo "${prefix}_${suffix}.tar.gz"
    fi
}

# Does a release carry this asset? Answers 0 for yes and 1 for a 404, and stops
# the installer on anything else: a rate limit or a network blip must not read
# as "this release predates the binary" and quietly skip it.
asset_exists() {
    local url="$1" code=""
    if command -v curl &>/dev/null; then
        code="$(curl -sL -o /dev/null -I -w '%{http_code}' "$url" || true)"
    elif command -v wget &>/dev/null; then
        # --spider's exit status folds 404 in with every other server error, so
        # read the status line itself; -S prints one per redirect hop.
        code="$(wget -S --spider -q -O /dev/null "$url" 2>&1 | awk '/^ *HTTP\// {c=$2} END {print c}')"
    fi

    case "$code" in
        200) return 0 ;;
        404) return 1 ;;
        *)   error "Could not check ${url} (HTTP ${code:-none}). Retry, or install components one at a time." ;;
    esac
}

# Build Rust maintenance worker asset name. No large-disk variant: the worker
# maintains tables through the namespace and never opens a volume file.
worker_asset_name() {
    local os="$1" arch="$2"
    echo "weed-worker_${os}_${arch}.tar.gz"
}

# Install a single component
install_component() {
    local component="$1" os="$2" arch="$3"
    local asset_name download_url tmpdir

    tmpdir="$(mktemp -d)"
    trap "rm -rf '$tmpdir'" EXIT

    case "$component" in
        weed)
            asset_name="$(go_asset_name "$os" "$arch")"
            download_url="https://github.com/${REPO}/releases/download/${VERSION}/${asset_name}"
            download "$download_url" "${tmpdir}/${asset_name}"

            info "Extracting ${asset_name}..."
            tar xzf "${tmpdir}/${asset_name}" -C "$tmpdir"

            # The Go release action puts the binary inside a directory
            local weed_bin
            weed_bin="$(find "$tmpdir" -name 'weed' -type f | head -1)"
            if [ -z "$weed_bin" ]; then
                weed_bin="$(find "$tmpdir" -name 'weed.exe' -type f | head -1)"
            fi
            if [ -z "$weed_bin" ]; then
                error "Could not find weed binary in archive"
            fi

            chmod +x "$weed_bin"
            install_binary "$weed_bin" "weed"
            ok "Installed weed to ${INSTALL_DIR}/weed"
            ;;

        volume-rust)
            # Check platform support for Rust volume server
            case "$os" in
                linux|darwin|windows) ;;
                *) error "Rust volume server is not available for ${os}. Supported: linux, darwin, windows" ;;
            esac
            case "$arch" in
                amd64|arm64) ;;
                *) error "Rust volume server is not available for ${arch}. Supported: amd64, arm64" ;;
            esac

            asset_name="$(rust_asset_name "$os" "$arch")"
            download_url="https://github.com/${REPO}/releases/download/${VERSION}/${asset_name}"
            download "$download_url" "${tmpdir}/${asset_name}"

            info "Extracting ${asset_name}..."
            if [ "$os" = "windows" ]; then
                unzip -q "${tmpdir}/${asset_name}" -d "$tmpdir"
            else
                tar xzf "${tmpdir}/${asset_name}" -C "$tmpdir"
            fi

            local rust_bin
            if [ "$LARGE_DISK" = true ]; then
                rust_bin="$(find "$tmpdir" -name 'weed-volume-large-disk*' -type f | head -1)"
            else
                rust_bin="$(find "$tmpdir" -name 'weed-volume-normal*' -type f | head -1)"
            fi
            if [ -z "$rust_bin" ]; then
                rust_bin="$(find "$tmpdir" -name 'weed-volume*' -type f | head -1)"
            fi
            if [ -z "$rust_bin" ]; then
                error "Could not find weed-volume binary in archive"
            fi

            chmod +x "$rust_bin"
            local dest_name="weed-volume"
            if [ "$os" = "windows" ]; then
                dest_name="weed-volume.exe"
            fi
            install_binary "$rust_bin" "$dest_name"
            ok "Installed weed-volume to ${INSTALL_DIR}/${dest_name}"
            ;;

        worker-rust)
            # Published for linux only: the worker runs beside the cluster it
            # maintains, and its dependency tree makes every extra target an
            # expensive build.
            case "$os" in
                linux) ;;
                *) error "Rust maintenance worker is not available for ${os}. Supported: linux" ;;
            esac
            case "$arch" in
                amd64|arm64) ;;
                *) error "Rust maintenance worker is not available for ${arch}. Supported: amd64, arm64" ;;
            esac

            asset_name="$(worker_asset_name "$os" "$arch")"
            download_url="https://github.com/${REPO}/releases/download/${VERSION}/${asset_name}"
            download "$download_url" "${tmpdir}/${asset_name}"

            info "Extracting ${asset_name}..."
            tar xzf "${tmpdir}/${asset_name}" -C "$tmpdir"

            local worker_bin
            worker_bin="$(find "$tmpdir" -name 'weed-worker' -type f | head -1)"
            if [ -z "$worker_bin" ]; then
                error "Could not find weed-worker binary in archive"
            fi

            chmod +x "$worker_bin"
            install_binary "$worker_bin" "weed-worker"
            WORKER_INSTALLED=true
            ok "Installed weed-worker to ${INSTALL_DIR}/weed-worker"
            ;;

        *)
            error "Unknown component: ${component}. Use: weed, volume-rust, worker-rust, all"
            ;;
    esac

    # The trap is per-process, so a later component's would replace this one and
    # leave the earlier extraction behind. Clean up here and hand the trap back;
    # the error paths above exit, which still fires it.
    rm -rf "$tmpdir"
    trap - EXIT
}

# Copy binary to install dir, using sudo if needed
install_binary() {
    local src="$1" name="$2"
    local dest="${INSTALL_DIR}/${name}"

    mkdir -p "$INSTALL_DIR" 2>/dev/null || true

    if [ -w "$INSTALL_DIR" ]; then
        cp "$src" "$dest"
    else
        info "Need elevated permissions to write to ${INSTALL_DIR}"
        sudo cp "$src" "$dest"
    fi
    chmod +x "$dest" 2>/dev/null || sudo chmod +x "$dest"
}

main() {
    info "SeaweedFS Installer"

    read -r os arch <<< "$(detect_platform)"
    info "Detected platform: ${os}/${arch}"

    if [ -z "$VERSION" ]; then
        info "Resolving latest release..."
        VERSION="$(get_latest_version)"
        if [ -z "$VERSION" ]; then
            error "Could not determine latest version. Specify with --version"
        fi
    fi
    info "Version: ${VERSION}"

    if [ "$LARGE_DISK" = true ]; then
        info "Variant: large disk (8TB max volume)"
    else
        info "Variant: normal (32GB max volume)"
    fi

    case "$COMPONENT" in
        all)
            install_component "weed" "$os" "$arch"
            install_component "volume-rust" "$os" "$arch"
            # The worker is published for linux amd64/arm64 only, and only by
            # releases new enough to carry it; skip either case rather than fail
            # an install that has already put two binaries in place.
            if [ "$os" != "linux" ] || { [ "$arch" != "amd64" ] && [ "$arch" != "arm64" ]; }; then
                warn "Skipping the Rust maintenance worker: no build for ${os}/${arch}"
            elif ! asset_exists "https://github.com/${REPO}/releases/download/${VERSION}/$(worker_asset_name "$os" "$arch")"; then
                warn "Skipping the Rust maintenance worker: ${VERSION} does not carry one"
            else
                install_component "worker-rust" "$os" "$arch"
            fi
            ;;
        *)
            install_component "$COMPONENT" "$os" "$arch"
            ;;
    esac

    echo ""
    ok "Installation complete!"
    if [ "$COMPONENT" = "weed" ] || [ "$COMPONENT" = "all" ]; then
        info "  weed:            ${INSTALL_DIR}/weed"
    fi
    if [ "$COMPONENT" = "volume-rust" ] || [ "$COMPONENT" = "all" ]; then
        info "  weed-volume:  ${INSTALL_DIR}/weed-volume"
    fi
    if [ "$WORKER_INSTALLED" = true ]; then
        info "  weed-worker:     ${INSTALL_DIR}/weed-worker"
    fi
    echo ""
    info "Quick start:"
    info "  weed master                          # Start master server"
    info "  weed volume -mserver=localhost:9333   # Start Go volume server"
    info "  weed-volume -mserver localhost:9333 # Start Rust volume server"
    if [ "$WORKER_INSTALLED" = true ]; then
        info "  weed-worker --admin localhost:23646  # Start the Rust maintenance worker"
    fi
}

main
