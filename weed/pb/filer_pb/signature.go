package filer_pb

func (r *CreateEntryRequest) AddSignature(sig int32) {
	r.Signatures = append(r.Signatures, sig)
}
func (r *CreateEntryRequest) HasSigned(sig int32) bool {
	for _, s := range r.Signatures {
		if s == sig {
			return true
		}
	}
	return false
}
