package filer_pb

import "context"

// ReadFilerDigest returns the digest a filer reports, empty for a filer that
// predates the setting. Writers that put chunks into a filer other than the
// one their process follows pass it to the chunk upload, so the digests they
// write match the filer's.
func ReadFilerDigest(ctx context.Context, client SeaweedFilerClient) (string, error) {
	resp, err := client.GetFilerConfiguration(ctx, &GetFilerConfigurationRequest{})
	if err != nil {
		return "", err
	}
	return resp.Digest, nil
}
