# dbqueue

> A distributed queue with a client server model.

## Overview

There are two major components of dbqueue:

### The Server

The majority of the action occurs on the Server, which is responsible for
actually handling messages. The Server uses an event loop (`mio`) to manage
client connections, acceptors, and timeouts, and Futures (`eventual`) to
synchronize events.

There is an example of setting up a default server in `tests/examples/server.rs`,
and a multi-threaded client example for load and performance testing the
server in `tests/examples/client.rs`.

#### Server Concurrency

The Server uses a single-threaded model by default, where it can avoid any
synchronization overhead in managing queues. This is more than sufficient for
most cases, but if you find that you need multithreading you can use a
`ConcurrentQueues` to manage a set of queues to share among multiple servers
by initializing them using the `with_queues` method.

There are examples of a multi-threaded server and a client to query it in
`tests/examples/multiserver.rs` and `tests/examples/mutliserver-client.rs`.

### The Client

There are two different clients available: a fully synchronous client which is
simple to use, and waits for a response to each request using blocking io, and
a second client which allows pipelining of requests so that many requests can
be sent without waiting for responses.

The pipelining client often allows the server to process several requests and
write several responses at once, and can increase performance. However, due to
some limitations in the eventual API (tracking https://github.com/carllerche/eventual/issues/19) it has a somewhat
clumsier API, requiring the user to keep track of the request and response order.

## Performance

The Server is able to handle a large number of connections efficiently, through
its use of a readiness-based event loop, `mio`. In fact, using the synchronous
Client, even when pipelining many requests, I was unable to get a single-thread
Server to use near 100% of the single core it was provided on my machine.

On my Mid 2014 Macbook Pro, running OS X 10.10, with a 2.8 Ghz i7, the server
completes `tests/examples/client.rs` at roughly `25000` request/response pairs
and validation per second.

To run load tests for a single threaded server, edit the constants in
`tests/examples/client.rs` to your liking, run `tests/examples/server.rs`
using `cargo run --release --example=server` from the tests directory
and then run the client using `cargo run --release --example=client`. It will
do a relatively naive benchmark and report the number of request/response
pairs made and the time it took.

The client and server use only one queue, but performance for multiple queues
is almost identical, since it is not really more expensive to manage multiple
queues than a single queue.

To run load tests for the multithreaded server and client, do the same as for
the single threaded server but run the `multiserver` and `multiserver-client`
examples instead. Take care to run with `--release`.

## Codebase

There are four crates involved in dbqueue, `dbqueue-server`, `dbqueue-client`,
`dqueue-common`, and `dbqueue-tests`. `dbqueue-server` contains the Server API,
`dbqueue-client` contains the Client APIs, `dbqueue-common` contains the
message types and their serialization and deserialization, and `dbqueue-tests`
has tests and examples.

There are a few correctness tests and a simple benchmark in `tests/examples/src/lib.rs`.

