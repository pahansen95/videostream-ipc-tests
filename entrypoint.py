import asyncio
import json
import sys
import os
import shutil
import time
import itertools
import subprocess
import math
from typing import Any, Coroutine, Iterable, Mapping, List, Tuple
from traceback import format_exc

def log(*msg: str):
  print(
    "\n".join(msg),
    file=sys.stderr,
  )

async def create_ffmpeg_async_proc(
  input_video: str,
  *output_filters: str,
) -> Tuple[asyncio.subprocess.Process, Tuple[str, ...]]:
  ffmpeg_cmd = "ffmpeg"
  ffmpeg_args = [
    "-loglevel", "error",
    "-re",
    "-i", input_video,
    "-f", "rawvideo",
    "-c:v", "rawvideo",
    "-pix_fmt", "rgb24",
    *[v for v in ["-vf", *output_filters] if output_filters],
    "pipe:1",
  ]
  return (
    await asyncio.create_subprocess_exec(
      ffmpeg_cmd,
      *ffmpeg_args,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
      # limit=frame_byte_size * 30, # Buffer up to 30 Frames (1 second)
    ),
    (ffmpeg_cmd, *ffmpeg_args),
  )

def create_ffmpeg_proc(
  input_video: str,
  *output_filters: str,
) -> Tuple[subprocess.Popen, Tuple[str, ...]]:
  ffmpeg_cmd = "ffmpeg"
  ffmpeg_args = [
    "-loglevel", "error",
    "-re",
    "-i", input_video,
    "-f", "rawvideo",
    "-c:v", "rawvideo",
    "-pix_fmt", "rgb24",
    *[v for v in ["-vf", *output_filters] if output_filters],
    "pipe:1",
  ]
  return (
    subprocess.Popen(
      args=[ffmpeg_cmd, *ffmpeg_args],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
    ),
    (ffmpeg_cmd, *ffmpeg_args),
  )


async def create_ffprobe_async_proc(
  input_video: str,
) -> asyncio.subprocess.Process:
  ffprobe_cmd = "ffprobe"
  ffprobe_args = [
    "-print_format", "json",
    "-v", "quiet",
    "-show_format",
    "-show_streams",
    input_video,
  ]
  return asyncio.create_subprocess_exec(
    ffprobe_cmd,
    *ffprobe_args,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    limit=2**32,
  )

def create_ffprobe_proc(
  input_video: str,
) -> subprocess.Popen:
  ffprobe_cmd = "ffprobe"
  ffprobe_args = [
    "-print_format", "json",
    "-v", "quiet",
    "-show_format",
    "-show_streams",
    input_video,
  ]
  return subprocess.Popen(
    [ffprobe_cmd, *ffprobe_args],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
  )

def probe_video_for_info(
  input_video: str,
) -> Mapping[str, Any]:
  ffprobe_proc = create_ffprobe_proc(
    input_video
  )
  return json.loads(
    ffprobe_proc.stdout.read()
  )

async def time_it_unfiltered(
  input_video: str,
):
  _buffer = b''
  ffmpeg_proc, ffmpeg_args = await create_ffmpeg_async_proc(input_video)
  stream = ffmpeg_proc.stdout
  log(
    f"Timing asyncio.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = await stream.read(-1)
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size: {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time: {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate: {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"\n",
  )
  if (await ffmpeg_proc.wait()) != 0:
    log(
      " ".join(ffmpeg_args),
      (await ffmpeg_proc.stderr.read(-1)).decode()
    )

  _buffer = b''
  ffmpeg_proc, ffmpeg_args = create_ffmpeg_proc(input_video)
  stream = ffmpeg_proc.stdout
  log(
    f"Timing io.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = stream.read()
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size: {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time: {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate: {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"\n",
  )
  if ffmpeg_proc.wait() != 0:
    log(
      " ".join(ffmpeg_args),
      ffmpeg_proc.stderr.read().decode()
    )


async def time_it_filtered(
  input_video: str,
  fps: int,
):
  # https://ffmpeg.org/ffmpeg-filters.html#fps-1
  fps_ffmpeg_filter=f"fps=fps={fps}"
  _buffer = b''
  ffmpeg_proc, ffmpeg_args = await create_ffmpeg_async_proc(
    input_video,
    fps_ffmpeg_filter,
  )
  stream = ffmpeg_proc.stdout
  log(
    f"Timing asyncio.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = await stream.read(-1)
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size: {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time: {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate: {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"\n",
  )
  if (await ffmpeg_proc.wait()) != 0:
    log(
      " ".join(ffmpeg_args),
      (await ffmpeg_proc.stderr.read(-1)).decode()
    )

  _buffer = b''
  ffmpeg_proc, ffmpeg_args = create_ffmpeg_proc(
    input_video,
    fps_ffmpeg_filter,
  )
  stream = ffmpeg_proc.stdout
  log(
    f"Timing io.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = stream.read()
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size: {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time: {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate: {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"\n",
  )
  if ffmpeg_proc.wait() != 0:
    log(
      " ".join(ffmpeg_args),
      ffmpeg_proc.stderr.read().decode()
    )

async def main(
  input_video: str,
  *all_target_fps: int,
):
  ### Get Input File Data ###
  video_info = probe_video_for_info(
    input_video
  )["streams"][0]

  resolution = (
    int(video_info["width"]),
    int(video_info["height"]),
  )
  _num, _denom = video_info["r_frame_rate"].split("/")
  fps = float(_num) / float(_denom)
  duration = float(video_info["duration"])
  ### Run Benchmarks ###

  ## Unfiltered
  log(
    "".join(["#" for _ in range(shutil.get_terminal_size().columns)]),
    f"### Producer Results @ {fps:.2f} FPS (unfiltered)",
    "".join(["#" for _ in range(shutil.get_terminal_size().columns)]),
    "\n",
  )
  log(
    f"Video Info",
    f"  Video Resolution: {resolution[0]}x{resolution[1]}p",
    f"  Video FPS: {fps:.2f}",
    f"  Frame Byte size: {math.prod([3, *resolution]) * 1E-6:.2f}MB",
    f"  Estimated Byte Rate: {math.prod([3, *resolution]) * 1E-6 * fps:.2f}MB/s",
    f"  Estimated Total Byte Size: {duration * math.prod([3, *resolution]) * 1E-6 * fps:.2f}MB",
    f"  Ideal Processing Time: {duration:.2f}s",
    f"\n",
  )
  await time_it_unfiltered(
    input_video
  )

  for target_fps in sorted(set(all_target_fps), reverse=True):
    ## Filtered
    if target_fps <= 0:
      # Filtering is disabled so skip it
      continue
    else:
      fps = target_fps
    log(
      "".join(["#" for _ in range(shutil.get_terminal_size().columns)]),
      f"### Producer Results @ {fps:.2f} FPS",
      "".join(["#" for _ in range(shutil.get_terminal_size().columns)]),
      "\n",
    )
    log(
      f"Video Info",
      f"  Video Resolution: {resolution[0]}x{resolution[1]}p",
      f"  Video FPS: {fps:.2f}",
      f"  Frame Byte size: {math.prod([3, *resolution]) * 1E-6:.2f}MB",
      f"  Estimated Byte Rate: {math.prod([3, *resolution]) * 1E-6 * fps:.2f}MB/s",
      f"  Estimated Total Byte Size: {duration * math.prod([3, *resolution]) * 1E-6 * fps:.2f}MB",
      f"  Ideal Processing Time: {duration:.2f}s",
      f"\n",
    )

    await time_it_filtered(
      input_video,
      fps
    )

if __name__ == "__main__":
  asyncio.run(
    main(
      sys.argv[1],
      *[float(fps) for fps in sys.argv[2:]],
    )
  )
