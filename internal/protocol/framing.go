package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const DefaultMaxFrameSize uint32 = 16 << 20

var (
	ErrFrameTooLarge = errors.New("frame exceeds size limit")
	ErrEmptyFrame    = errors.New("empty frame")
)

// Codec's zero value uses DefaultMaxFrameSize. Callers serialize writes and
// provide connection deadlines. A framing or I/O error makes the stream unsafe
// to reuse; a validation error after a fully read frame does not lose framing.
type Codec struct{ MaxFrameSize uint32 }

func (c Codec) limit() uint32 {
	if c.MaxFrameSize == 0 {
		return DefaultMaxFrameSize
	}
	return c.MaxFrameSize
}

func (c Codec) ReadFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return nil, ErrEmptyFrame
	}
	if size > c.limit() || uint64(size) > uint64(^uint(0)>>1) {
		return nil, ErrFrameTooLarge
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return data, nil
}

func (c Codec) WriteFrame(w io.Writer, data []byte) error {
	if len(data) == 0 {
		return ErrEmptyFrame
	}
	if uint64(len(data)) > uint64(c.limit()) {
		return ErrFrameTooLarge
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if err := writeFull(w, header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if err := writeFull(w, data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// ReadRequest returns the decoded ID alongside validation errors when usable.
func (c Codec) ReadRequest(r io.Reader) (Request, error) {
	data, err := c.ReadFrame(r)
	if err != nil {
		return Request{}, err
	}
	return decodeRequest(data)
}

func (c Codec) ReadResponse(r io.Reader) (Response, error) {
	data, err := c.ReadFrame(r)
	if err != nil {
		return Response{}, err
	}
	return decodeResponse(data)
}

func (c Codec) WriteRequest(w io.Writer, r Request) error {
	if err := r.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.WriteFrame(w, data)
}

func (c Codec) WriteResponse(w io.Writer, r Response) error {
	if err := r.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.WriteFrame(w, data)
}
