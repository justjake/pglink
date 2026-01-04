package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var ErrNoNullTerminator = errors.New("pgwire: no null terminator")

type MessageHeader struct {
	MsgType MsgType
	Len     int32
}

type MessageParser struct{ Rest []byte }

func (p *MessageParser) NextByte() byte {
	b := p.Rest[0]
	p.Rest = p.Rest[1:]
	return b
}

func (p *MessageParser) SkipBytes(n int) {
	p.Rest = p.Rest[n:]
}

func (p *MessageParser) HasBytes(n int) bool {
	return len(p.Rest) >= n
}

func (p *MessageParser) ReadBytes(n int) []byte {
	b := p.Rest[:n]
	p.Rest = p.Rest[n:]
	return b
}

func (p *MessageParser) ReadInt32() int32 {
	return int32(binary.BigEndian.Uint32(p.ReadBytes(4)))
}

func (p *MessageParser) ReadMessageHeader() MessageHeader {
	msgType := p.NextByte()
	len := p.ReadInt32()
	return MessageHeader{
		MsgType: MsgType(msgType),
		Len:     len,
	}
}

func (p *MessageParser) ReadString() (string, error) {
	nullTerminatorIdx := bytes.IndexByte(p.Rest, 0)
	if nullTerminatorIdx < 0 {
		return "", ErrNoNullTerminator
	}
	data := p.ReadBytes(nullTerminatorIdx)
	p.SkipBytes(1) // skip null terminator
	return string(data), nil
}
