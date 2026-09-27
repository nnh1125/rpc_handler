// Package operations contains computation independent of network transport.
package operations

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashParams.Data is encoded as a base64 JSON string by encoding/json.
type HashParams struct {
	Data []byte `json:"data"`
}
type HashResult struct {
	Hex string `json:"hex"`
}

func HashCompute(params HashParams) HashResult {
	digest := sha256.Sum256(params.Data)
	return HashResult{Hex: hex.EncodeToString(digest[:])}
}
