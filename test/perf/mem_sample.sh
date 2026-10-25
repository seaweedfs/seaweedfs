#!/usr/bin/env bash
# Sample resident memory of one or more processes once per second.
# Usage: mem_sample.sh <out.csv> <label=pid> [label=pid ...]
#
# Writes a per-second time series to <out.csv> and, on stop, the peak
# resident set (VmHWM) per process to <out.csv>.peak. Stop with SIGTERM/SIGINT;
# if a process already exited, its peak falls back to the max RSS sampled.
# Linux only (reads /proc/<pid>/status).
set -u

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <out.csv> <label=pid> [label=pid ...]" >&2
  exit 1
fi

out="$1"; shift

labels=(); pids=(); peaks=()
for arg in "$@"; do
  labels+=("${arg%%=*}")
  pids+=("${arg#*=}")
  peaks+=(0)
done

record_peaks() {
  local total=0 i hwm
  : > "${out}.peak"
  for i in "${!pids[@]}"; do
    hwm="$(awk '/^VmHWM:/{print $2}' "/proc/${pids[$i]}/status" 2>/dev/null)"
    if [ -z "$hwm" ] || [ "$hwm" -eq 0 ]; then
      hwm="${peaks[$i]}"
    fi
    printf '%s\t%s\n' "${labels[$i]}" "$hwm" >> "${out}.peak"
    total=$((total + hwm))
  done
  printf 'total\t%s\n' "$total" >> "${out}.peak"
  exit 0
}
trap record_peaks TERM INT

header="epoch"
for l in "${labels[@]}"; do header+=",${l}_rss_kb"; done
header+=",total_rss_kb"
echo "$header" > "$out"

while true; do
  line="$(date +%s)"; total=0
  for i in "${!pids[@]}"; do
    p="${pids[$i]}"
    rss="$(awk '/^VmRSS:/{print $2}' "/proc/$p/status" 2>/dev/null)"
    rss="${rss:-0}"
    if [ "$rss" -gt "${peaks[$i]}" ]; then
      peaks[$i]="$rss"
    fi
    line+=",${rss}"
    total=$((total + rss))
  done
  echo "${line},${total}" >> "$out"
  # Sleep via a background child + wait so a caught signal interrupts promptly.
  sleep 1 &
  wait $! 2>/dev/null
done
