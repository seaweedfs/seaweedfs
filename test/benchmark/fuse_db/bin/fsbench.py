#!/usr/bin/env python3
# Raw filesystem microbenchmark in one directory. Prints TSV: METRIC<TAB>value<TAB>unit
#   SEQWRITE  MB/s  (512MB stream + final fsync)
#   FSYNC     ops/s (500 x: pwrite 4KB at random offset in a 64MB file, then fsync)
#   SEQREAD   MB/s  (read the 512MB file back; warm cache)
import sys, os, time, random

d = sys.argv[1]
os.makedirs(d, exist_ok=True)
big = os.path.join(d, "fsb_big.bin")
sm  = os.path.join(d, "fsb_sync.bin")

# SEQWRITE 512MB + fsync
n = 512 * 1024 * 1024
buf = os.urandom(8 * 1024 * 1024)
t = time.time(); f = open(big, "wb"); w = 0
while w < n:
    f.write(buf); w += len(buf)
f.flush(); os.fsync(f.fileno()); f.close()
dt = time.time() - t
print(f"SEQWRITE\t{w/1048576/dt:.0f}\tMB/s")

# FSYNC latency: 500 x (pwrite 4KB random offset + fsync) on a 64MB file
fd = os.open(sm, os.O_RDWR | os.O_CREAT, 0o644)
os.ftruncate(fd, 64 * 1024 * 1024)
os.fsync(fd)
page = os.urandom(4096)
N = 500
rnd = random.Random(12345)
t = time.time()
for _ in range(N):
    off = rnd.randrange(0, 64 * 1024 * 1024 - 4096) & ~0xfff
    os.pwrite(fd, page, off)
    os.fsync(fd)
dt = time.time() - t
os.close(fd)
print(f"FSYNC\t{N/dt:.0f}\tops/s")
print(f"FSYNCLAT\t{dt/N*1000:.2f}\tms")

# SEQREAD (warm)
t = time.time(); tot = 0
with open(big, "rb") as f:
    while True:
        b = f.read(8 * 1024 * 1024)
        if not b: break
        tot += len(b)
dt = time.time() - t
print(f"SEQREAD\t{tot/1048576/dt:.0f}\tMB/s")

for p in (big, sm):
    try: os.remove(p)
    except OSError: pass
