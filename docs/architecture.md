# Architecture and semantics

## Version 0.1 boundary

`@gdguesser/graphql-sqs-pubsub` 0.1 is a direct-SQS adapter for one consuming
server process. There must be only one active `SQSPubSub` instance for a given
queue across the deployment.

SQS gives each message to one consumer. It cannot broadcast one queue message to
every replica. Process-local fan-out fixes competing listeners inside one
GraphQL server process; it does not change SQS semantics between processes.

## Components

```text
producer
   |
   | JSON body + trigger message attribute
   v
existing SQS queue
   |
   | one abortable long poll
   v
SQSPubSub worker
   |
   | parse once; select local registry by trigger
   +----------+----------+
   v          v          v
iterator A  iterator B  iterator C
   |          |          |
   +----- GraphQL execution / withFilter ----->
                              WebSocket clients
```

The trigger registry contains a map per trigger and a unique numeric ID for each
listener. `unsubscribe(id)` removes only that listener. Removing the final
listener for a trigger removes the empty map but does not stop the worker.

## Receive loop

Each instance owns at most one worker:

1. call `ReceiveMessage` with an abort signal;
2. report and back off after receive errors;
3. process every message in the returned batch sequentially;
4. issue the next long poll until `close()` aborts the signal.

Sequential batch processing avoids adding local reordering. It does not add an
ordering guarantee that the underlying standard queue does not provide.

`start()` cannot create a second worker. `close()` waits for an already-received
batch to finish local processing, destroys a package-owned AWS client, and is
safe to call repeatedly. Closing is terminal; a closed instance cannot restart.

## Message state machine

```text
received
   |
   +-- missing receipt handle ----------> no delete; retry / DLQ
   +-- missing or empty trigger --------> no delete; retry / DLQ
   +-- invalid or missing JSON body ----> no delete; retry / DLQ
   |
   v
valid live event
   |
   +-- zero listeners ------------------> delete
   |
   v
parallel local enqueue to trigger listeners
   |
   +-- any listener rejected -----------> report; no delete; retry / DLQ
   |
   v
DeleteMessage
   |
   +-- delete failed --------------------> report; SQS may redeliver
   v
consumed
```

The same parsed object is offered to every listener in the snapshot. Listener
failures are isolated with `Promise.allSettled`, so one failure does not stop
other listeners from receiving the event. The failed message is not deleted,
which preserves at-least-once behavior and can duplicate delivery to listeners
that previously succeeded.

GraphQL's async iterator enqueue is the local acknowledgement boundary. The
package cannot know whether a remote client later receives or processes the
WebSocket frame.

## Transport seam

The fan-out engine depends internally on a small `MessageTransport` contract:

- `receive(signal)`
- `acknowledge(message)`
- `publish(trigger, serializedBody, options)`
- `close()`

Version 0.1 instantiates only `SqsTransport`. The seam keeps AWS command details
out of the registry and iterator logic and allows a future SNS publishing /
per-replica-SQS receiving transport without duplicating fan-out behavior. It is
intentionally not exported as a supported extension API yet; its shape may
change before 1.0.

No SNS command, topic management, queue subscription, policy mutation, or
resource provisioning exists in version 0.1.

## Concurrency boundaries

- Listener registry mutations are synchronous on the Node.js event loop.
- A dispatch snapshots listeners. An unsubscribe racing with that snapshot can
  still receive the current event but not later events.
- Listener callbacks for one message run concurrently.
- Messages in one received batch are processed sequentially.
- A slow iterator enqueue delays delete and the next message. Set queue
  visibility timeout with this upper bound in mind.
- `graphql-subscriptions` iterators buffer locally. Version 0.1 has no bounded
  per-client queue or remote-client acknowledgement.

## Delivery model

The complete path has different guarantees:

- producer to SQS: determined by producer retry behavior;
- SQS receive: at least once, with standard-queue duplication/reordering;
- worker to local iterator: attempted for every trigger listener before delete;
- GraphQL execution and `withFilter`: local, after broker acknowledgement
  eligibility;
- server to WebSocket client: best-effort, without application acknowledgement.

There is no end-to-end exactly-once claim. Applications should use event or
entity IDs to make terminal effects idempotent and should query canonical state
after connecting or reconnecting.

## Deployment invariant

Keep one active consumer for each direct queue, including during deployments.
For Kubernetes this normally means one replica and a strategy that prevents
surge overlap, such as `maxSurge: 0` and `maxUnavailable: 1`, or `Recreate`.
That can create a brief WebSocket interruption, so clients must reconnect and
reconcile canonical state.

When zero-downtime multi-replica broadcast is required, direct SQS is the wrong
topology. Follow the [roadmap](roadmap.md) rather than scaling this queue's
consumer count.
