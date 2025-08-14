#!/bin/bash

TARGET_DIR="$1"

(
    cd "$TARGET_DIR" || exit

    echo "Generating memray flamegraphs..."

    for filename in *; do
        echo "-> Processing $filename..."
        memray flamegraph "$filename"
    done
)

echo "All flamegraphs have been generated at $TARGET_DIR"
