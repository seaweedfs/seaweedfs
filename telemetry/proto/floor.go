package proto

// MinDiskBytes is what a cluster must store before its master reports it and
// the server keeps the report: fresh `weed server` runs, CI jobs and throwaway
// containers each mint their own cluster id, and they were most of the counted
// clusters while holding almost none of the bytes.
const MinDiskBytes uint64 = 10 << 30
