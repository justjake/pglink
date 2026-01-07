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

func (p *MessageParser) NextByte() (byte, error) {
	if len(p.Rest) == 0 {
		return 0, ErrMsgTooShort
	}
	b := p.Rest[0]
	p.Rest = p.Rest[1:]
	return b, nil
}

func (p *MessageParser) SkipBytes(n int) error {
	if len(p.Rest) < n {
		return ErrMsgTooShort
	}
	p.Rest = p.Rest[n:]
	return nil
}

func (p *MessageParser) HasBytes(n int) bool {
	return len(p.Rest) >= n
}

func (p *MessageParser) ReadBytes(n int) ([]byte, error) {
	if len(p.Rest) < n {
		return nil, ErrMsgTooShort
	}
	b := p.Rest[:n]
	p.Rest = p.Rest[n:]
	return b, nil
}

func (p *MessageParser) ReadUint32() (uint32, error) {
	b, err := p.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (p *MessageParser) ReadInt32() (int32, error) {
	u, err := p.ReadUint32()
	return int32(u), err
}

func (p *MessageParser) SkipMessageHeader() error {
	return p.SkipBytes(5)
}

func (p *MessageParser) ReadMessageHeader() (MessageHeader, error) {
	msgType, err := p.NextByte()
	if err != nil {
		return MessageHeader{}, err
	}
	len, err := p.ReadInt32()
	if err != nil {
		return MessageHeader{}, err
	}
	return MessageHeader{
		MsgType: MsgType(msgType),
		Len:     len,
	}, nil
}

func (p *MessageParser) ReadString() (string, error) {
	nullTerminatorIdx := bytes.IndexByte(p.Rest, 0)
	if nullTerminatorIdx < 0 {
		return "", ErrNoNullTerminator
	}
	data, err := p.ReadBytes(nullTerminatorIdx)
	if err != nil {
		return "", err
	}
	// skip null terminator
	if err = p.SkipBytes(1); err != nil {
		return "", err
	}
	return string(data), nil
}
