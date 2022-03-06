import asyncio
from asyncore import write
from ctypes import Union
import json
import socket
import sys
import os
import select
import shutil
import time
import itertools
import subprocess
import math
from functools import partial
from enum import IntEnum
from typing import Any, Coroutine, Iterable, Literal, Mapping, List, Tuple, Dict
from traceback import format_exc

class ConnType(IntEnum):
  PIPELINE = 1
  UNIX_SOCKET = 2

def log(*msg: str):
  if "NO_LOG" not in os.environ:
    print(
      "\n".join([f"{m}" for m in msg]),
      file=sys.stderr,
    )

def p_stdout(*msg: str):
  print(
    "\n".join([f"{m}" for m in msg]),
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
  target_rate: float,
):
  _buffer = b''
  ffmpeg_proc, ffmpeg_args = await create_ffmpeg_async_proc(input_video)
  stream = ffmpeg_proc.stdout
  log(
    f"\nTiming asyncio.{type(stream).__name__}[{type(stream._transport).__name__}].read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = await stream.read(-1)
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size:          {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time:         {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate:          {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"  Byte Rate Scale:    {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9) / target_rate:.0%}",
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
    f"\nTiming io.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = stream.read()
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size:          {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time:         {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate:          {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"  Byte Rate Scale:    {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9) / target_rate:.0%}",
  )
  if ffmpeg_proc.wait() != 0:
    log(
      " ".join(ffmpeg_args),
      ffmpeg_proc.stderr.read().decode()
    )


async def time_it_filtered(
  input_video: str,
  fps: int,
  target_rate: float,
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
    f"\nTiming asyncio.{type(stream).__name__}[{type(stream._transport).__name__}].read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = await stream.read(-1)
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size:          {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time:         {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate:          {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"  Byte Rate Scale:    {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9) / target_rate:.0%}",
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
    f"\nTiming io.{type(stream).__name__}.read()...",
  )
  start_time = time.monotonic_ns()
  _buffer = stream.read()
  end_time = time.monotonic_ns()
  log(
    f"  Byte Size:          {len(_buffer) * 1E-6:.2f} MB",
    f"  Total Time:         {(end_time - start_time) * 1E-9:.2f} s",
    f"  Byte Rate:          {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9):.2f} MB/s",
    f"  Byte Rate Scale:    {(len(_buffer) * 1E-6) / ((end_time - start_time) * 1E-9) / target_rate:.0%}",
  )
  if ffmpeg_proc.wait() != 0:
    log(
      " ".join(ffmpeg_args),
      ffmpeg_proc.stderr.read().decode()
    )

async def time_it_generic(
  input_video: str,
  fps: int,
  conn_type: ConnType,
  expected_byte_size: int,
  read_chunk_size: int,
):
  ### Setup ###

  output: Dict[str, Dict] = {
    "blocking": {},
    "non-blocking": {},
    "asyncio": {},
  }

  ffmpeg_filters = [v for v in ["-vf", f"{fps}"] if fps]
  ffmpeg_cmd = "ffmpeg"
  common_ffmpeg_args = [
    "-loglevel", "error",
    "-stream_loop", "-1", # Loop Forever
    "-re",  # Read input at the native frame rate
    "-i", input_video,
    "-f", "rawvideo",
    "-c:v", "rawvideo",
    "-pix_fmt", "rgb24",
    *ffmpeg_filters,
  ]
  log(" ".join([ffmpeg_cmd, *common_ffmpeg_args]))

  ### Blocking ###

  if conn_type == ConnType.PIPELINE:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, True)
    os.set_inheritable(write_fd, True)
    _pass_fds = (write_fd,)
    ffmpeg_output = f"pipe:{write_fd}"
    read_file = open(read_fd, mode='rb')
    read_bytes = partial(read_file.read, read_chunk_size)
    close_it = read_file.close
  elif conn_type == ConnType.UNIX_SOCKET:
    socket_addr = f"/tmp/{time.monotonic_ns()}.unix"
    server_socket = socket.socket(
      family=socket.AF_UNIX,
      type=socket.SOCK_STREAM,
    )
    server_socket.bind(socket_addr)
    server_socket.listen()
    _pass_fds = ()
    ffmpeg_output = [
      f"unix://{socket_addr}",
    ]
  else:
    raise TypeError(conn_type)

  # Create the Popen Process
  ffmpeg_proc = subprocess.Popen(
    args=[ffmpeg_cmd, *common_ffmpeg_args, ffmpeg_output],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.PIPE,
    pass_fds=_pass_fds,
  )

  if conn_type == ConnType.UNIX_SOCKET:
    # Accept the connection
    client_socket, _ = server_socket.accept()
    client_socket.setblocking(True)
    read_bytes = partial(client_socket.recv, read_chunk_size)
    close_it = client_socket.close
  
  try:
    assert ffmpeg_proc.poll() is None, f"{ffmpeg_proc.returncode}"
  except:
    log(ffmpeg_proc.stderr.read().decode())
  
  total_bytes_read: int = 0
  total_time_read: int = 0
  while True:
    time_datum = time.monotonic_ns()
    _bytes = read_bytes()
    total_time_read += time.monotonic_ns() - time_datum
    total_bytes_read += len(_bytes)
    if total_bytes_read >= expected_byte_size:
      break
  
  if ffmpeg_proc.poll() is None:
    ffmpeg_proc.terminate()
    ffmpeg_proc.wait()
  try:
    assert not ffmpeg_proc.poll(), f"{ffmpeg_proc.returncode}"
  except:
    log(ffmpeg_proc.stderr.read().decode())

  close_it()

  output["blocking"] = {
    "bytes_read": total_bytes_read,
    "reading_time": total_time_read,
  }

  ### Non-Blocking ###
  
  """
  A Note to myself...
    I'm just blocking until data can be written again.
    Not really useful here but if background tasks needed to be done
      this allows for asynchronous work
  """
  log(
    f"{select.POLLIN=}",
    f"{select.POLLPRI=}",
    f"{select.POLLOUT=}",
    f"{select.POLLERR=}",
    f"{select.POLLHUP=}",
    f"{select.POLLRDHUP=}",
    f"{select.POLLNVAL=}",
    f"{select.POLLMSG=}",
  )

  poll = select.poll()
  if conn_type == ConnType.PIPELINE:
    # See O_NONBLOCK information see -> https://man7.org/linux/man-pages/man2/open.2.html
    # For Polling information see -> https://man7.org/linux/man-pages/man2/poll.2.html
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_inheritable(write_fd, True)
    poll.register(read_fd, select.POLLIN | select.POLLPRI)
    read_file = open(read_fd, mode='rb')
    read_bytes = partial(read_file.read, read_chunk_size)
    close_it = read_file.close
    ffmpeg_output = f"pipe:{write_fd}"
    _pass_fds = (write_fd,)
  elif conn_type == ConnType.UNIX_SOCKET:
    socket_addr = f"/tmp/{time.monotonic_ns()}.unix"
    server_socket = socket.socket(
      family=socket.AF_UNIX,
      type=socket.SOCK_STREAM,
    )
    server_socket.bind(socket_addr)
    server_socket.listen()
    _pass_fds = ()
    ffmpeg_output = [
      f"unix://{socket_addr}",
    ]
  else:
    raise TypeError(conn_type)

  # Create the Popen Process
  ffmpeg_proc = subprocess.Popen(
    args=[ffmpeg_cmd, *common_ffmpeg_args, ffmpeg_output],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.PIPE,
    pass_fds=_pass_fds,
  )
  
  try:
    assert ffmpeg_proc.poll() is None, f"{ffmpeg_proc.returncode}"
  except:
    log(ffmpeg_proc.stderr.read().decode())

  data_available_bits = {select.POLLIN, select.POLLPRI}
  peer_closed_bit = {select.POLLHUP,}
  eof_flag = True  
  total_bytes_read: int = 0
  total_time_read: int = 0
  while True:
    poll_results = poll.poll()
    if poll_results:
      # log(f"{poll_results=}")
      if poll_results[0][1] in data_available_bits:
        # log(f"data available")
        time_datum = time.monotonic_ns()
        _bytes = read_bytes()
        end_time = time.monotonic_ns()
        if len(_bytes) > 0:
          # log(f"{len(_bytes)=}")
          total_bytes_read += len(_bytes)
          total_time_read += end_time - time_datum
          if total_bytes_read >= expected_byte_size:
            # log("All Bytes Read")
            break
        else:
          if eof_flag:
            break
          else:
            eof_flag = True
        # else pass
      elif poll_results[0][1] in peer_closed_bit:
        # (for a pipe) peer closed it's end
        if eof_flag:
          break
        else:
          eof_flag = True
      else:
        raise RuntimeError(poll_results[0][1])
    else:
      # log("No Poll Results")
      continue

  if ffmpeg_proc.poll() is None:
    ffmpeg_proc.terminate()
    ffmpeg_proc.wait()
  try:
    assert not ffmpeg_proc.poll(), f"{ffmpeg_proc.returncode}"
  except:
    log(ffmpeg_proc.stderr.read().decode())

  close_it()

  output["non-blocking"] = {
    "bytes_read": total_bytes_read,
    "reading_time": total_time_read,
  }

  ### Async ###
  # TODO
  pass
  
  return output  

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
  frame_byte_size = math.prod([3, *resolution])
  log(frame_byte_size)
  _num, _denom = video_info["r_frame_rate"].split("/")
  fps = float(_num) / float(_denom)
  log(fps)
  duration = 30
  log(duration)
  chunk_size = 2**12 # 4096 Bytes (4KiB)
  log(chunk_size)

  results = {
    "pipeline": {},
    "unix_socket": {},
  }

  # Determine all the target FPS to run
  all_target_fps: List[int] = sorted(
    set(
      [
        target_fps
        for target_fps in all_target_fps
        if target_fps < fps and target_fps > 0 
      ]
    ),
    reverse=True
  )
  all_target_fps.insert(0,0) # Unfiltered
  log(all_target_fps)

  ### Run Benchmarks ###
  header = f"### General Info ###"
  p_stdout(
    "".join(["#" for _ in range(len(header))]),
    header,
    "".join(["#" for _ in range(len(header))]),
  )
  p_stdout(
    f"\nVideo Info",
    f"  Video Resolution            {resolution[0]}x{resolution[1]}p",
    f"  Video FPS                   {fps:.2f}",
    f"  Frame Byte Size             {frame_byte_size / 2**20:.2f} MiB",
    f"\n",
  )

  for target_fps in all_target_fps:
    if target_fps == 0:
      target_fps = fps
    ## Calculate Numbers
    total_byte_size = round(duration * target_fps) * frame_byte_size
    log(total_byte_size)
    real_duration = total_byte_size / (frame_byte_size * target_fps)
    log(real_duration)
    target_byte_rate = total_byte_size / real_duration
    log(target_byte_rate)
    ## Unfiltered
    if target_fps == fps:
      header = f"### Producer Results @ {fps:.2f} FPS (unfiltered) ###"
    else:
      header = f"### Producer Results @ {target_fps} FPS ###"
    p_stdout(
      "".join(["#" for _ in range(len(header))]),
      header,
      "".join(["#" for _ in range(len(header))]),
    )
    p_stdout(
      f"\nProfiling Info",
      f"  Bytes Per Chunk             {chunk_size} B",
      f"  Target Byte Rate            {target_byte_rate / 2**20:.2f} MiB/s",
      f"  Target Bytes Read           {total_byte_size / 2**20:.2f}MiB",
      f"  Calculated Read Time        {real_duration:.2f}s",
    )
    p_stdout(
      "\nPipeline Tests"
    )
    results["pipeline"] = await time_it_generic(
      input_video,
      0, # Unfiltered
      ConnType.PIPELINE,
      total_byte_size,
      chunk_size,
    )

    p_stdout(
      "  Blocking:",
      f"    Actual Bytes Read:    {results['pipeline']['blocking']['bytes_read'] / 2**20:.2f}Mib",
      f"    Total Time Reading:   {results['pipeline']['blocking']['reading_time'] / 1E9:.2f}s",
      "  Non-Blocking:",
      f"    Actual Bytes Read:    {results['pipeline']['non-blocking']['bytes_read'] / 2**20:.2f}Mib",
      f"    Total Time Reading:   {results['pipeline']['non-blocking']['reading_time'] / 1E9:.2f}s",
      "  Asyncio:",
      "    N/A",
      "\n",
    )

if __name__ == "__main__":
  asyncio.run(
    main(
      sys.argv[1],
      *[float(fps) for fps in sys.argv[2:]],
    )
  )
