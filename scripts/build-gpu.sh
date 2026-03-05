#!/usr/bin/env bash
# Build and test LTSeq GPU version in Docker
#
# Requires: NVIDIA Container Toolkit (nvidia-docker2 or nvidia-container-toolkit)
#
# Usage:
#   ./scripts/build-gpu.sh          # build + test
#   ./scripts/build-gpu.sh build    # build only
#   ./scripts/build-gpu.sh test     # test only (requires prior build + GPU)
#   ./scripts/build-gpu.sh shell    # interactive shell with GPU access

set -euo pipefail

IMAGE="ltseq:gpu"
DOCKERFILE="docker/Dockerfile.gpu"
CONTEXT="."

cd "$(git rev-parse --show-toplevel)"

case "${1:-all}" in
    build)
        echo "==> Building GPU image..."
        docker build -f "$DOCKERFILE" -t "$IMAGE" "$CONTEXT"
        echo "==> Done. Image: $IMAGE"
        ;;
    test)
        echo "==> Running tests in $IMAGE (with GPU access)..."
        docker run --rm --gpus all "$IMAGE"
        ;;
    shell)
        echo "==> Starting interactive shell in $IMAGE (with GPU access)..."
        docker run --rm -it --gpus all "$IMAGE" /bin/bash
        ;;
    all)
        echo "==> Building GPU image..."
        docker build -f "$DOCKERFILE" -t "$IMAGE" "$CONTEXT"
        echo ""
        echo "==> Running tests (with GPU access)..."
        docker run --rm --gpus all "$IMAGE"
        echo ""
        echo "==> All done."
        ;;
    *)
        echo "Usage: $0 [build|test|shell|all]"
        exit 1
        ;;
esac
