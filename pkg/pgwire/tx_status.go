package pgwire

type TxStatus byte

const (
	TxIdle          TxStatus = 'I'
	TxInTransaction TxStatus = 'T'
	TxFailed        TxStatus = 'E'
	// TxActive        TxStatus = 'A'
)
