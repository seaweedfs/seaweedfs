package http

// func NewFilerHttpClient(opt ...HttpClientOpt)(*HTTPClient, error) {
// 	return NewHttpClient(Filer, opt...)
// }

// func NewVolumeHttpClient(opt ...HttpClientOpt)(*HTTPClient, error) {
// 	return NewHttpClient(Volume, opt...)
// }

// func NewMasterHttpClient(opt ...HttpClientOpt)(*HTTPClient, error) {
// 	return NewHttpClient(Master, opt...)
// }

func NewGlobalHttpClient(opt ...HttpClientOpt)(*HTTPClient, error) {
	return NewHttpClient(Global, opt...)
}
