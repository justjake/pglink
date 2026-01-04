// Package pgproxy provides abstractions for writing PostgreSQL protocol proxy applications.
// The main entry point is [Session], which proxies messages between a [Frontend] and a [Backend],
// but makes no assumptions about message handling policy.
//
// There are several [MessageTracker] implementations to assist implementing proxy logic.
package pgproxy
