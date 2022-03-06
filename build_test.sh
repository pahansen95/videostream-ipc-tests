#!/usr/bin/env bash

docker build \
  -t rtsp-test \
  .

docker run \
  -it \
  --rm \
  -v "${PWD}/logs":/camera/logs \
  rtsp-test