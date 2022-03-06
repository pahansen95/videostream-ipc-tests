#!/usr/bin/env bash

set -Eeuo pipefail

export NO_LOG=1

docker build \
  -t rtsp-test \
  . \
  1>/dev/null 2>&1

{
  docker run \
    -it \
    --rm \
    --env NO_LOG \
    --entrypoint python3 \
    rtsp-test \
      ./entrypoint.py \
        ./video.mp4 \
          60s \
          25fps 20fps 15fps 10fps 5fps 1fps 
} | tee ./results.txt