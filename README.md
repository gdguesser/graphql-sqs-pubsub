# @gdguesser/graphql-sqs-pubsub

Direct Amazon SQS transport for GraphQL subscriptions, with one long-poll worker
and process-local fan-out.

> [!IMPORTANT] Version 0.1 supports **exactly one consuming server process per
> queue**. Multiple listeners inside that process receive the same event.
> Multiple processes or replicas would compete for SQS messages and must not
> share the queue. This direct-SQS boundary is a live-notification path, not
> durable state: the database or source service remains authoritative, and
> clients must reconcile by querying canonical state on connection and
> reconnection. See [Architecture](docs/architecture.md) and
> [Roadmap](docs/roadmap.md).

## Why this package exists

SQS is a competing-consumer queue, not a broadcast broker. Starting one SQS
poller for every GraphQL subscription allows the wrong subscriber to receive and
delete an event before GraphQL filtering rejects it.

This package starts one abortable long poll per `SQSPubSub` instance. It parses
each message once and fans it out to every local iterator registered for the
message trigger.

## Install

Node.js 22 or newer is required.

```sh
npm install @gdguesser/graphql-sqs-pubsub graphql graphql-subscriptions
```

`graphql` and `graphql-subscriptions` are peers. `@aws-sdk/client-sqs` is a
runtime dependency because the package provides a default AWS SDK v3 client
path.

## Message contract

Published and consumed messages use:

- a JSON message body;
- a String message attribute named `SQSPubSubTriggerName` by default;
- the GraphQL trigger name as that attribute's `StringValue`.

The attribute name can be changed with `triggerAttribute` for migration from an
existing producer.

## Basic use

```ts
import { SQSPubSub } from "@gdguesser/graphql-sqs-pubsub";

const pubsub = new SQSPubSub({
  queueUrl: process.env.SQS_QUEUE_URL!,
  clientConfig: { region: process.env.AWS_REGION },
  observer(event) {
    // Safe metadata only: no payload, body, receipt handle, or raw error.
    metrics.record(event);
  },
});

await pubsub.publish("ORDER_UPDATED", {
  orderUpdated: { id: "order-42", status: "complete" },
});

const resolvers = {
  Subscription: {
    orderUpdated: {
      subscribe: () => pubsub.asyncIterableIterator("ORDER_UPDATED"),
    },
  },
};

process.once("SIGTERM", () => {
  void pubsub.close();
});
```

`autoStart` defaults to `true`. Set it to `false` when listeners must be
registered before consuming begins, then call the idempotent `start()` method.
`close()` is also idempotent and aborts an in-flight long poll.

The current example in [`examples/apollo-server.ts`](examples/apollo-server.ts)
uses Apollo Server 5, `graphql-ws`, and `withFilter`. Its test connects two
independent WebSocket clients and verifies argument-based filtering.

## Existing AWS SDK v3 client

```ts
import { SQSClient } from "@aws-sdk/client-sqs";
import { SQSPubSub } from "@gdguesser/graphql-sqs-pubsub";

const client = new SQSClient({ region: "eu-west-1" });
const pubsub = new SQSPubSub({
  queueUrl: process.env.SQS_QUEUE_URL!,
  client,
});
```

Injected clients are not destroyed by default. Set `destroyClientOnClose: true`
only when this instance owns the injected client. Providing both `client` and
`clientConfig` is rejected.

## Standard and FIFO publishing

Standard queue behavior is the default. A queue URL whose queue name ends with
`.fifo` is safely recognized as FIFO. FIFO can also be selected explicitly:

```ts
const pubsub = new SQSPubSub({
  queueUrl: process.env.SQS_QUEUE_URL!,
  queueType: "fifo",
  fifo: {
    messageGroupId: "graphql-events",
  },
});

await pubsub.publish(
  "ORDER_UPDATED",
  { orderUpdated: { id: "order-42" } },
  {
    messageGroupId: "order-42",
    messageDeduplicationId: "order-42-version-7",
  },
);
```

When omitted in FIFO mode, the group ID defaults to the trigger and the
deduplication ID defaults to a random UUID. This works whether or not
content-based deduplication is enabled. FIFO options on an explicitly standard
queue are rejected rather than silently ignored. Supplying `fifo` defaults does
not silently change a standard queue into FIFO mode: use a `.fifo` queue URL or
set `queueType: "fifo"`.

## Processing and deletion semantics

For each received message, the worker:

1. validates the receipt handle and trigger attribute;
2. parses the JSON body exactly once;
3. snapshots every local listener for that trigger;
4. enqueues the same parsed value to all listeners, isolating failures;
5. deletes the SQS message only when every local enqueue succeeds.

Valid messages with zero listeners are live events and are deleted. They are not
saved for a future WebSocket client.

Malformed JSON, a missing trigger, or a missing receipt handle is not deleted.
SQS can retry the message and move it to a configured dead-letter queue.
Listener or delete failures are reported through the observer and can cause
redelivery. A successful listener can therefore see a duplicate when another
listener failed.

## Delivery guarantees

- Standard SQS queues are at least once and can duplicate or reorder events.
- FIFO queues improve broker ordering and deduplication but do not make
  WebSocket delivery exactly once.
- Delivery from the process to a connected GraphQL/WebSocket client is
  best-effort. There is no client acknowledgement in this abstraction.
- Disconnects can lose live notifications. On reconnect, query canonical
  application state.
- The package intentionally performs no hidden queue, DLQ, topic, subscription,
  or IAM provisioning.

Applications must make terminal UI notifications and state transitions
idempotent.

## Observer API

`observer` receives structured metadata with:

- `phase`: lifecycle, receive, validate, dispatch, listener, delete, or publish;
- `outcome`: started, succeeded, failed, or skipped;
- optional `messageId`, `trigger`, listener/message counts, subscription ID, and
  error category.

It never receives a payload, body, receipt handle, or raw error. Trigger names
are included and should therefore be static identifiers, never usernames, tenant
IDs, or other user data. Observers may be synchronous or asynchronous; the
worker does not await them, and both thrown errors and rejected promises are
isolated from broker processing.

## Main API

`new SQSPubSub(options)`

- `queueUrl` — required existing queue URL.
- `client` or `clientConfig` — injected SQS client or configuration for a
  package-created `SQSClient`.
- `autoStart` — starts the worker during construction; default `true`.
- `waitTimeSeconds` — SQS long-poll duration, `0..20`; default `20`.
- `maxNumberOfMessages` — receive batch size, `1..10`; default `10`.
- `visibilityTimeout` — optional per-receive visibility timeout.
- `receiveErrorBackoffMs` — delay after receive errors; default `1000`.
- `triggerAttribute` — default `SQSPubSubTriggerName`.
- `queueType` — `auto`, `standard`, or `fifo`; default `auto`.
- `fifo` — FIFO publish defaults and an optional deduplication ID factory.
- `observer` — payload-free lifecycle and processing events.

Methods:

- `start(): void`
- `close(): Promise<void>`
- `publish(trigger, payload, options?): Promise<void>`
- `subscribe(trigger, callback, options): Promise<number>`
- `unsubscribe(subscriptionId): void`
- `asyncIterableIterator<T>(trigger | triggers)`
- `asyncIterator<T>(trigger | triggers)` — compatibility alias
- `getSubscriberStats(trigger?)`

Only public `graphql-subscriptions` APIs are imported.

## Least-privilege IAM

The consuming and publishing process needs only the actions it uses:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:SendMessage"],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:QUEUE_NAME"
    }
  ]
}
```

Add `sqs:ChangeMessageVisibility` only if the application separately uses it.
This package does not require queue creation, queue discovery, purge, or
attribute-management permissions.

## Local development

Unit and Apollo/WebSocket tests do not need Docker:

```sh
npm test
npm run test:example
```

The real-SQS integration test is opt-in:

```sh
npm run localstack:up
npm run test:integration
npm run localstack:down
```

The integration test creates and deletes only its own temporary LocalStack
queue. Production code never provisions resources.

## Migration

See [Migration guide](docs/migration.md) for
`thegreatercurve/graphql-sqs-subscriptions`, the `gdguesser` compatibility fork,
constructor changes, shutdown, and deployment safeguards.

## Troubleshooting

**Only some replicas receive events:** direct SQS is being used with more than
one consuming process. Scale to one, prevent rollout overlap, or give each
process a distinct queue populated by a true fan-out transport.

**Malformed messages repeat:** this is intentional. Correct the producer or
configure an SQS redrive policy and inspect the DLQ.

**Messages redeliver after listeners ran:** inspect `listener` and `delete`
observer failures. Successful local delivery does not compensate for a failed
delete.

**Events disappear while no clients are connected:** valid messages are live
events and are consumed. Query canonical state on connection and reconnect.

**FIFO publish is rejected:** ensure the queue URL ends in `.fifo` or set
`queueType: "fifo"`. Do not send FIFO fields to a standard queue.

## Project documents

- [Architecture and semantics](docs/architecture.md)
- [Migration guide](docs/migration.md)
- [Roadmap](docs/roadmap.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)

## License and attribution

MIT. The original `graphql-sqs-subscriptions` work is attributed to John
Flockton. This standalone implementation and new work are attributed to Gabriel
Dietrich Guesser. See [LICENSE](LICENSE).
