package vr

import (
	"bufio"
	"encoding/gob"
	"io"
	"net/rpc"
)

// this is currently just a copy of the gorpc gobServerCodec code (which is not public)
// but we can use this to insert hooks into parts of the rpc process/synchronize things

type GobServerCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
}

func (c *GobServerCodec) ReadRequestHeader(r *rpc.Request) error {
	err := c.dec.Decode(r)
	return err
}

func (c *GobServerCodec) ReadRequestBody(body interface{}) error {
	return c.dec.Decode(body)
	// RPC implementation will start
}

func (c *GobServerCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {
	// RPC implementation just ended
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return c.encBuf.Flush()
}

func (c *GobServerCodec) Close() error {
	return c.rwc.Close()
}
