#!/bin/bash
# Package all project files for submission

ARCHIVE="distributed_service_submission.zip"
rm -f "$ARCHIVE"

zip -r "$ARCHIVE" \
    cmd/ \
    raft/ \
    kvraft/ \
    python/ \
    scripts/ \
    screenshots/ \
    Makefile \
    go.mod \
    README.md \
    .gitignore \
    -x "*.DS_Store*" -x "*bin/*" -x "*.log" -x "*.pid"

echo "Created $ARCHIVE successfully."
