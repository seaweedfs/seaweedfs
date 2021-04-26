#!/bin/bash

hsbench -a accesstoken -s secret -z 4K -d 10 -t 10 -b 10 -u http://localhost:8333 -m "cxipgdx" -bp "hsbench-"
