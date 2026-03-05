#!/usr/bin/env bash
# Build and test LTSeq CPU version in Docker
#
# Usage:
#   ./scripts/build-cpu.sh          # build + test
#   ./scripts/build-cpu.sh build    # build only
#   ./scripts/build-cpu.sh test     # test only (requires prior build)
#   ./scripts/build-cpu.sh shell    # interactive shell in container

set -euo pipefail

IMAGE="ltseq:cpu"
DOCKERFILE="docker/Dockerfile.cpu"
CONTEXT="."

cd "$(git rev-parse --show-toplevel)"

case "${1:-all}" in
    build)
        echo "==> Building CPU image..."
        docker build -f "$DOCKERFILE" -t "$IMAGE" "$CONTEXT"
        echo "==> Done. Image: $IMAGE"
        ;;
    test)
        echo "==> Running tests in $IMAGE..."
        docker run --rm "$IMAGE"
        ;;
    shell)
        echo "==> Starting interactive shell in $IMAGE..."
        docker run --rm -it "$IMAGE" /bin/bash
        ;;
    all)
        echo "==> Building CPU image..."
        docker build -f "$DOCKERFILE" -t "$IMAGE" "$CONTEXT"
        echo ""
        echo "==> Running tests..."
        docker run --rm "$IMAGE"
        echo ""
        echo "==> All done."
        ;;
    *)
        echo "Usage: $0 [build|test|shell|all]"
        exit 1
        ;;
esac
