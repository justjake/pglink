package pgwire

type TxStatus byte

const (
	TxIdle          TxStatus = 'I'
	TxActive        TxStatus = 'A'
	TxInTransaction TxStatus = 'T'
	TxFailed        TxStatus = 'E'
)
