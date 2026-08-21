package format

// Adapters register from init(), so the map needs no locking.
var formats = map[string]Format{}

func Register(f Format) {
	if _, ok := formats[f.Name()]; ok {
		panic("format: duplicate adapter " + f.Name())
	}
	formats[f.Name()] = f
}

// ByName returns the registered adapter, or nil.
func ByName(name string) Format {
	return formats[name]
}
