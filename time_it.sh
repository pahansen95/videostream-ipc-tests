#!/usr/bin/env bash

declare \
  video_file="${1:?Must Specify an Input Video File}"

time ffmpeg -v quiet -re -i "${video_file}" -f rawvideo -c:v rawvideo -pix_fmt rgb24 pipe:1 | time dd 1>/dev/null