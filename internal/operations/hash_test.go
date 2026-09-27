package operations

import "testing"

func TestHashCompute(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	} {
		if got := HashCompute(HashParams{Data: []byte(tc.input)}); got.Hex != tc.want {
			t.Errorf("hash(%q) = %q", tc.input, got.Hex)
		}
	}
}
