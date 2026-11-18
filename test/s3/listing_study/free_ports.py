#!/usr/bin/env python3
"""Print N free TCP ports whose +10000 gRPC companions are free too."""

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
while len(picked) < int(sys.argv[1]):
    port = random.randrange(20000, 55000)
    if all(abs(port - p) > 20 for p in picked) and free(port) and free(port + 10000):
        picked.append(port)
print(*picked)
