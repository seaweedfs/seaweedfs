#!/usr/bin/env python3
"""Print N free TCP ports whose +10000 gRPC companions are free too, all below
the Linux ephemeral range (32768+) that outgoing connections draw from."""

import random
import socket
import sys


def free(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


picked: list[int] = []
taken: list[int] = []
while len(picked) < int(sys.argv[1]):
    port = random.randrange(10000, 22000)
    pair = (port, port + 10000)
    if all(abs(p - t) > 20 for p in pair for t in taken) and all(free(p) for p in pair):
        picked.append(port)
        taken += pair
print(*picked)
