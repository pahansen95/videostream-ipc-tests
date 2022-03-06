FROM aler9/rtsp-simple-server:v0.17.16 AS rtsp
FROM python:3.9-alpine3.15

RUN mkdir /camera

# ENV PATH="/usr/local/bin:${PATH}"
COPY --from=rtsp /rtsp-simple-server /usr/local/bin/
# COPY --from=rtsp /rtsp-simple-server.yml /camera/
COPY ./rtsp-simple-server.yaml /camera/

RUN apk add --no-cache \
  ffmpeg \
  bash \
  curl \
  jq

COPY ./*video.mp4 ./time_it.sh ./entrypoint.sh ./entrypoint.py /camera/

RUN chmod +x /camera/entrypoint.sh /camera/time_it.sh

WORKDIR /camera

ENTRYPOINT [ "bash" ]

CMD [ "-i" ]
