#!/bin/bash

proj=$1

case $proj in
    "shared")
        cd $(git rev-parse --show-toplevel)/libs/shared
        ;;
    "worker")
        cd $(git rev-parse --show-toplevel)/apps/worker
        ;;
    "codecov-api")
        cd $(git rev-parse --show-toplevel)/apps/codecov-api
        ;;
    "umbrella")
        cd $(git rev-parse --show-toplevel)
        ;;
    *)
        echo "Unknown uv project"
        exit 1
        ;;
esac

uv sync --frozen

if [ "$(git diff --name-only uv.lock)" != '' ]; then
    echo "\`uv sync\` made new changes for $proj. Please review and then commit again."
    exit 1
fi

echo "All good"
exit 0
