package pgwire

import (
	"errors"

	"github.com/jackc/pgio"
)

type MessageEncoder struct {
	Buffer []byte
}

func (e *MessageEncoder) Start(msgType MsgType) int {
	dst, sp := beginMessage(e.Buffer, byte(msgType))
	e.Buffer = dst
	return sp
}

func (e *MessageEncoder) WriteByte(b byte) {
	e.Buffer = append(e.Buffer, b)
}

func (e *MessageEncoder) WriteBytes(b []byte) {
	e.Buffer = append(e.Buffer, b...)
}

func (e *MessageEncoder) WriteInt32(n int32) {
	e.Buffer = pgio.AppendInt32(e.Buffer, n)
}

func (e *MessageEncoder) WriteString(s string) {
	e.Buffer = append(e.Buffer, s...)
	e.Buffer = append(e.Buffer, 0)
}

func (e *MessageEncoder) End(sp int) error {
	dst, err := finishMessage(e.Buffer, sp)
	if err != nil {
		return err
	}
	e.Buffer = dst
	return nil
}

func (e *MessageEncoder) WriteMessage(msg RawMessageSource) error {
	sp := e.Start(msg.MessageType())
	e.WriteBytes(msg.MessageBody())
	return e.End(sp)
}

// Copyright (c) 2019 Jack Christensen
//
// # MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// maxMessageBodyLen is the maximum length of a message body in bytes. See PG_LARGE_MESSAGE_LIMIT in the PostgreSQL
// source. It is defined as (MaxAllocSize - 1). MaxAllocSize is defined as 0x3fffffff.
const maxMessageBodyLen = (0x3fffffff - 1)

// Copyright (c) 2019 Jack Christensen
//
// # MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// beginMessage begins a new message of type t. It appends the message type and a placeholder for the message length to
// dst. It returns the new buffer and the position of the message length placeholder.
func beginMessage(dst []byte, t byte) ([]byte, int) {
	dst = append(dst, t)
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)
	return dst, sp
}

// Copyright (c) 2019 Jack Christensen
//
// # MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// finishMessage finishes a message that was started with beginMessage. It computes the message length and writes it to
// dst[sp]. If the message length is too large it returns an error. Otherwise it returns the final message buffer.
func finishMessage(dst []byte, sp int) ([]byte, error) {
	messageBodyLen := len(dst[sp:])
	if messageBodyLen > maxMessageBodyLen {
		return nil, errors.New("message body too large")
	}
	pgio.SetInt32(dst[sp:], int32(messageBodyLen))
	return dst, nil
}
