package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// receiveWithDeadline receives a message with a deadline to prevent tests from hanging forever.
// This ensures tests fail fast if the proxy stalls or forgets to respond.
func receiveWithDeadline(t *testing.T, conn *pgconn.PgConn, frontend *pgproto3.Frontend, deadline time.Duration) (pgproto3.BackendMessage, error) {
	t.Helper()
	netConn := conn.Conn()
	if err := netConn.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}
	defer netConn.SetReadDeadline(time.Time{}) // Clear deadline after
	return frontend.Receive()
}

// TestPipelinedUnnamedStatement tests the scenario that caused pgbench to fail
// with 0 TPS: sending Parse+Bind+Execute+Sync for the unnamed statement in a
// single pipelined batch.
//
// This test would fail before the fix with errors like:
// - "statement not in cache for re-creation" (HasStatement bug)
// - Sessions timing out with outstanding requests (passthrough flush bug)
func TestPipelinedUnnamedStatement(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Connect using pgconn for low-level protocol access
	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable",
		PredefinedUsers.App.Username, PredefinedUsers.App.Password, h.Port(), "alpha_uno")
	conn, err := pgconn.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	frontend := conn.Frontend()

	// Send a pipelined batch like pgbench does:
	// Parse (unnamed) + Bind (unnamed) + Execute + Sync
	//
	// This is the pattern that exposed two bugs:
	// 1. HasStatement didn't check PendingCreate, so Bind thought the statement
	//    didn't exist and tried to re-create it from cache (which failed)
	// 2. Passthrough handlers didn't flush messages, so they never reached backend

	// Parse unnamed statement
	frontend.Send(&pgproto3.Parse{
		Name:  "",
		Query: "SELECT $1::int + $2::int AS sum",
	})

	// Bind to unnamed portal
	frontend.Send(&pgproto3.Bind{
		DestinationPortal:    "",
		PreparedStatement:    "",
		ParameterFormatCodes: []int16{0, 0}, // text format
		Parameters:           [][]byte{[]byte("21"), []byte("21")},
		ResultFormatCodes:    []int16{0}, // text format
	})

	// Execute
	frontend.Send(&pgproto3.Execute{
		Portal:  "",
		MaxRows: 0,
	})

	// Sync
	frontend.Send(&pgproto3.Sync{})

	// Flush the batch
	err = frontend.Flush()
	require.NoError(t, err)

	// Read responses with deadline - test fails fast if proxy stalls
	// Expected: ParseComplete, BindComplete, DataRow, CommandComplete, ReadyForQuery
	var gotParseComplete, gotBindComplete, gotDataRow, gotCommandComplete, gotReadyForQuery bool
	var result string
	const readDeadline = 5 * time.Second

	for i := 0; i < 10; i++ { // Safety limit
		msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
		require.NoError(t, err, "timed out waiting for response - proxy may have stalled")

		switch m := msg.(type) {
		case *pgproto3.ParseComplete:
			gotParseComplete = true
		case *pgproto3.BindComplete:
			gotBindComplete = true
		case *pgproto3.DataRow:
			gotDataRow = true
			if len(m.Values) > 0 {
				result = string(m.Values[0])
			}
		case *pgproto3.CommandComplete:
			gotCommandComplete = true
		case *pgproto3.ReadyForQuery:
			gotReadyForQuery = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("Got error response: %s (code: %s)", m.Message, m.Code)
		}

		if gotReadyForQuery {
			break
		}
	}

	require.True(t, gotParseComplete, "should receive ParseComplete")
	require.True(t, gotBindComplete, "should receive BindComplete")
	require.True(t, gotDataRow, "should receive DataRow")
	require.True(t, gotCommandComplete, "should receive CommandComplete")
	require.True(t, gotReadyForQuery, "should receive ReadyForQuery")
	require.Equal(t, "42", result, "21 + 21 should equal 42")
}

// TestPipelinedUnnamedStatementMultipleBatches tests repeated pipelined batches
// using the unnamed statement, similar to what pgbench does in a transaction.
func TestPipelinedUnnamedStatementMultipleBatches(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable",
		PredefinedUsers.App.Username, PredefinedUsers.App.Password, h.Port(), "alpha_uno")
	conn, err := pgconn.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	frontend := conn.Frontend()

	const readDeadline = 5 * time.Second

	// Run multiple pipelined batches (like pgbench transactions)
	for i := 0; i < 5; i++ {
		// Each batch: Parse + Bind + Execute + Sync
		frontend.Send(&pgproto3.Parse{
			Name:  "",
			Query: "SELECT $1::int * 2 AS doubled",
		})
		frontend.Send(&pgproto3.Bind{
			DestinationPortal:    "",
			PreparedStatement:    "",
			ParameterFormatCodes: []int16{0},
			Parameters:           [][]byte{[]byte("10")},
			ResultFormatCodes:    []int16{0},
		})
		frontend.Send(&pgproto3.Execute{Portal: "", MaxRows: 0})
		frontend.Send(&pgproto3.Sync{})

		err = frontend.Flush()
		require.NoError(t, err)

		// Consume responses with deadline
		var gotReadyForQuery bool
		for j := 0; j < 10; j++ {
			msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
			require.NoError(t, err, "batch %d: timed out waiting for response", i)

			if _, ok := msg.(*pgproto3.ErrorResponse); ok {
				t.Fatalf("batch %d: got error response: %v", i, msg)
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				gotReadyForQuery = true
				break
			}
		}
		require.True(t, gotReadyForQuery, "batch %d should complete with ReadyForQuery", i)
	}
}

// TestPipelinedWithTransaction tests pipelined extended query within a transaction,
// which is exactly what pgbench's tpcb workload does.
func TestPipelinedWithTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable",
		PredefinedUsers.App.Username, PredefinedUsers.App.Password, h.Port(), "alpha_uno")
	conn, err := pgconn.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	frontend := conn.Frontend()
	const readDeadline = 5 * time.Second

	// BEGIN transaction using unnamed statement
	frontend.Send(&pgproto3.Parse{Name: "", Query: "BEGIN"})
	frontend.Send(&pgproto3.Bind{DestinationPortal: "", PreparedStatement: ""})
	frontend.Send(&pgproto3.Execute{Portal: ""})
	frontend.Send(&pgproto3.Sync{})
	require.NoError(t, frontend.Flush())

	// Wait for ReadyForQuery with deadline
	for i := 0; i < 10; i++ {
		msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
		require.NoError(t, err, "BEGIN: timed out waiting for response")
		if errResp, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("BEGIN failed: %s", errResp.Message)
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			require.Equal(t, byte('T'), rfq.TxStatus, "should be in transaction")
			break
		}
	}

	// SELECT inside transaction
	frontend.Send(&pgproto3.Parse{Name: "", Query: "SELECT 1 AS one"})
	frontend.Send(&pgproto3.Bind{DestinationPortal: "", PreparedStatement: ""})
	frontend.Send(&pgproto3.Execute{Portal: ""})
	frontend.Send(&pgproto3.Sync{})
	require.NoError(t, frontend.Flush())

	var gotDataRow bool
	for i := 0; i < 10; i++ {
		msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
		require.NoError(t, err, "SELECT: timed out waiting for response")
		if errResp, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("SELECT failed: %s", errResp.Message)
		}
		if _, ok := msg.(*pgproto3.DataRow); ok {
			gotDataRow = true
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			require.Equal(t, byte('T'), rfq.TxStatus)
			break
		}
	}
	require.True(t, gotDataRow, "should receive data row from SELECT")

	// COMMIT transaction
	frontend.Send(&pgproto3.Parse{Name: "", Query: "COMMIT"})
	frontend.Send(&pgproto3.Bind{DestinationPortal: "", PreparedStatement: ""})
	frontend.Send(&pgproto3.Execute{Portal: ""})
	frontend.Send(&pgproto3.Sync{})
	require.NoError(t, frontend.Flush())

	for i := 0; i < 10; i++ {
		msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
		require.NoError(t, err, "COMMIT: timed out waiting for response")
		if errResp, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("COMMIT failed: %s", errResp.Message)
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			require.Equal(t, byte('I'), rfq.TxStatus, "should be idle after commit")
			break
		}
	}
}

// TestPipelinedDescribeInBatch tests Parse+Describe+Sync pipelining which is
// another pattern that exercises the passthrough flush fix.
func TestPipelinedDescribeInBatch(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable",
		PredefinedUsers.App.Username, PredefinedUsers.App.Password, h.Port(), "alpha_uno")
	conn, err := pgconn.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	frontend := conn.Frontend()

	// Send Parse + Describe + Sync in one batch
	frontend.Send(&pgproto3.Parse{
		Name:  "test_stmt",
		Query: "SELECT $1::int AS num, $2::text AS str",
	})
	frontend.Send(&pgproto3.Describe{
		ObjectType: 'S',
		Name:       "test_stmt",
	})
	frontend.Send(&pgproto3.Sync{})

	err = frontend.Flush()
	require.NoError(t, err)

	var gotParseComplete, gotParameterDesc, gotRowDesc, gotReadyForQuery bool
	const readDeadline = 5 * time.Second

	for i := 0; i < 10; i++ {
		msg, err := receiveWithDeadline(t, conn, frontend, readDeadline)
		require.NoError(t, err, "timed out waiting for response - proxy may have stalled")

		switch m := msg.(type) {
		case *pgproto3.ParseComplete:
			gotParseComplete = true
		case *pgproto3.ParameterDescription:
			gotParameterDesc = true
			require.Len(t, m.ParameterOIDs, 2, "should have 2 parameters")
		case *pgproto3.RowDescription:
			gotRowDesc = true
			require.Len(t, m.Fields, 2, "should have 2 fields")
			require.Equal(t, "num", string(m.Fields[0].Name))
			require.Equal(t, "str", string(m.Fields[1].Name))
		case *pgproto3.ReadyForQuery:
			gotReadyForQuery = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("Got error: %s (code: %s)", m.Message, m.Code)
		}

		if gotReadyForQuery {
			break
		}
	}

	require.True(t, gotParseComplete, "should receive ParseComplete")
	require.True(t, gotParameterDesc, "should receive ParameterDescription")
	require.True(t, gotRowDesc, "should receive RowDescription")
	require.True(t, gotReadyForQuery, "should receive ReadyForQuery")
}
