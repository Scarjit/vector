#!/usr/bin/env bash
set -euo pipefail

REGISTRY="ghcr.io/scarjit/vector"
VERSION="0.56.0"
DATETIME=$(date -u +"%Y%m%d-%H%M%S")
TAG_VERSIONED="${VERSION}-${DATETIME}"

echo "==> Building debug binary..."
cargo build

echo "==> Copying binary to build stage..."
cp target/debug/vector .build-stage/vector

echo "==> Building Docker image..."
docker build -f Dockerfile.custom \
  -t "${REGISTRY}:latest" \
  -t "${REGISTRY}:${TAG_VERSIONED}" \
  .

echo "==> Pushing ${REGISTRY}:latest ..."
docker push "${REGISTRY}:latest"

echo "==> Pushing ${REGISTRY}:${TAG_VERSIONED} ..."
docker push "${REGISTRY}:${TAG_VERSIONED}"

echo "==> Done. Tags pushed:"
echo "    ${REGISTRY}:latest"
echo "    ${REGISTRY}:${TAG_VERSIONED}"
