#!/usr/bin/env bash
# Robust auto-download for MongoDB Kafka Sink connector (multi-version, validates size & retries)
set -euo pipefail

# Resolve repo root (parent of this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REQUESTED_VERSION="${MONGO_CONNECTOR_VERSION:-}"  # If empty we'll iterate candidates
TARGET_DIR="${REPO_ROOT}/kafka/connect/plugins/mongodb"
MIN_SIZE=1000000 # 1 MB sanity threshold (jar is ~8-10MB normally)

CANDIDATE_VERSIONS=(
  "${REQUESTED_VERSION}"
  1.13.0
  1.12.0
  1.11.2
  1.11.1
  1.11.0
)

FILTERED_VERSIONS=()
for v in "${CANDIDATE_VERSIONS[@]}"; do
  [[ -n "$v" ]] && FILTERED_VERSIONS+=("$v")
done
if [[ ${#FILTERED_VERSIONS[@]} -eq 0 ]]; then
  echo "[mongo-connector] No versions to try (internal error)."; exit 1
fi

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log() { echo -e "${GREEN}[mongo-connector]${NC} $1"; }
warn() { echo -e "${YELLOW}[mongo-connector]${NC} $1"; }
err() { echo -e "${RED}[mongo-connector]${NC} $1"; }

mkdir -p "${TARGET_DIR}"

download_version() {
  local ver=$1
  local jar="mongo-kafka-connect-${ver}-all.jar"
  local primary="https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/${ver}/${jar}"
  local fallback="https://search.maven.org/remotecontent?filepath=org/mongodb/kafka/mongo-kafka-connect/${ver}/${jar}"

  if [[ -f "${TARGET_DIR}/${jar}" ]]; then
    local size
    size=$(stat -f%z "${TARGET_DIR}/${jar}" 2>/dev/null || stat -c%s "${TARGET_DIR}/${jar}" 2>/dev/null || echo 0)
    if [[ $size -gt ${MIN_SIZE} ]]; then
      log "Version ${ver} already downloaded (${size} bytes)."
      echo "${jar}" > "${TARGET_DIR}/.active_jar"
      return 0
    else
      warn "Corrupt/incomplete jar for ${ver} (${size} bytes). Re-downloading."
      rm -f "${TARGET_DIR}/${jar}"
    fi
  fi

  warn "Attempting download of version ${ver}..."
  if command -v curl >/dev/null 2>&1; then
    if ! curl -fL --retry 3 --retry-delay 2 -o "${TARGET_DIR}/${jar}" "$primary"; then
      warn "Primary URL failed for ${ver}, trying fallback..."
      curl -fL --retry 3 --retry-delay 2 -o "${TARGET_DIR}/${jar}" "$fallback" || return 1
    fi
  elif command -v wget >/dev/null 2>&1; then
    if ! wget -O "${TARGET_DIR}/${jar}" "$primary"; then
      warn "Primary URL failed for ${ver}, trying fallback..."
      wget -O "${TARGET_DIR}/${jar}" "$fallback" || return 1
    fi
  else
    err "Need curl or wget to download connector."; return 1
  fi

  local size
  size=$(stat -f%z "${TARGET_DIR}/${jar}" 2>/dev/null || stat -c%s "${TARGET_DIR}/${jar}" 2>/dev/null || echo 0)
  if [[ $size -lt ${MIN_SIZE} ]]; then
    warn "Downloaded file for ${ver} too small (${size} bytes). Deleting."
    rm -f "${TARGET_DIR}/${jar}" || true
    return 1
  fi

  log "Successfully downloaded ${jar} (${size} bytes)."
  echo "${jar}" > "${TARGET_DIR}/.active_jar"
  return 0
}

SUCCESS=0
for ver in "${FILTERED_VERSIONS[@]}"; do
  if download_version "$ver"; then
    SUCCESS=1
    ACTIVE_JAR=$(cat "${TARGET_DIR}/.active_jar")
    log "Active connector jar: ${ACTIVE_JAR}"
    break
  else
    warn "Version ${ver} failed. Trying next candidate..."
  fi
done

if [[ $SUCCESS -ne 1 ]]; then
  err "Failed to download any MongoDB Kafka connector version."
  exit 1
fi

log "Restart kafka-connect container to load/refresh plugin if it's already running."