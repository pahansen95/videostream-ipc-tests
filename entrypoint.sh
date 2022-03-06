#!/usr/bin/env bash

set -Eeuo pipefail

ffmpeg \
  -i ./video.mp4 \
  -c:v libx264 \
  -preset fast \
  -vsync passthrough \
  -vf "scale=-1:720" \
  -c:a aac \
  './video_720p.mp4'

# Server
{
  rtsp-simple-server \
    "./rtsp-simple-server.yaml" \
  2> logs/rtsp-simple-server.log
} &
server_pid="$!"

# Producer
{
  ffmpeg \
    -loglevel debug \
    -re \
    -stream_loop -1 \
    -i './video_720p.mp4' \
    -c copy \
    -f rtsp \
    rtsp://localhost:8554/test \
  2> "./logs/ffmpeg-producer.log"
} &
producer_pid="$!"

until curl --fail --silent http://localhost:9997/v1/paths/list | jq -e '.items.test'; do
  sleep 1
done

# Consumer
{
  ffmpeg \
    -loglevel debug \
    -i rtsp://localhost:8554/test \
    -f null \
    - \
  2> "./logs/ffmpeg-consumer.log"
} &
consumer_pid="$!"

wait -f "${consumer_pid}" "${producer_pid}"

kill -s TERM "${server_pid}"