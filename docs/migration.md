# Migration guide

This guide covers the original `thegreatercurve/graphql-sqs-subscriptions`
package and the `gdguesser/graphql-sqs-subscriptions` compatibility fork.

## Behavioral change to understand first

Older implementations commonly started an SQS consumer for every GraphQL async
iterator. SQS delivers a message to one consumer, so concurrent iterators
competed and a nonmatching `withFilter` subscription could receive and discard
another user's event.

The new package has one consumer per `SQSPubSub` instance and fans out by
trigger inside the process. This requires one active instance for the queue
across the entire deployment.

## Package and constructor

Replace the old package:

```ts
import { SQSPubSub } from "graphql-sqs-subscriptions";

const pubsub = new SQSPubSub(
  { region: process.env.AWS_REGION },
  process.env.SQS_QUEUE_URL,
);
```

with:

```ts
import { SQSPubSub } from "@gdguesser/graphql-sqs-pubsub";

const pubsub = new SQSPubSub({
  queueUrl: process.env.SQS_QUEUE_URL!,
  clientConfig: {
    region: process.env.AWS_REGION,
  },
});
```

The queue URL is required. The package does not create, discover, delete, purge,
or configure a production queue. Provision the queue and DLQ with the
application's infrastructure tooling.

For a shared or specially configured AWS SDK v3 client:

```ts
const pubsub = new SQSPubSub({
  queueUrl,
  client: sqsClient,
});
```

Injected clients remain open after `close()` unless `destroyClientOnClose: true`
is explicitly set.

## Resolver API

The current public API is:

```ts
subscribe: () => pubsub.asyncIterableIterator("RESULT_READY");
```

The older spelling remains as an alias:

```ts
subscribe: () => pubsub.asyncIterator("RESULT_READY");
```

Move new code to `asyncIterableIterator`, which is the `graphql-subscriptions` 3
API. Both return native `AsyncIterableIterator` objects.

`publish(trigger, payload)` remains available. FIFO callers can pass the
optional third argument documented in the README.

## Trigger and payload compatibility

The default trigger message attribute remains:

```text
SQSPubSubTriggerName
```

The message body remains JSON. Existing producers do not need a payload change
when they already write that attribute. Use `triggerAttribute` if an existing
producer uses a different attribute name.

Before rollout, capture representative messages in a nonproduction queue and
verify:

- the body is valid JSON;
- the trigger attribute has `DataType: String`;
- its `StringValue` exactly matches the resolver trigger;
- the queue visibility timeout and redrive policy are suitable.

## Singleton and shutdown

Create one long-lived instance per queue, not one instance per resolver,
request, user, or WebSocket:

```ts
export const resultsPubSub = new SQSPubSub({
  queueUrl: process.env.RESULTS_QUEUE_URL!,
  clientConfig: { region: process.env.AWS_REGION },
});
```

Call `close()` from the server's graceful-shutdown path:

```ts
async function shutdown(): Promise<void> {
  await resultsPubSub.close();
  await webSocketServer.dispose();
}
```

The exact shutdown order depends on the GraphQL server. Stop accepting new
connections, dispose active subscriptions, close the pub/sub worker, and then
close the HTTP server.

## Deployment safeguard

Direct mode cannot have overlapping consumers, including a brief overlap during
a rolling deployment.

For a one-replica Kubernetes deployment, use a no-surge strategy such as:

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 0
    maxUnavailable: 1
```

Alternatively use `Recreate`. Expect a reconnect window and make clients query
canonical state after reconnecting.

Do not scale the deployment beyond one consuming process until every replica has
its own queue fed by a broadcast service. SNS is not implemented in version 0.1.

## Filtering

Keep coarse routing in the trigger and application/user routing in GraphQL
`withFilter`. The engine dispatches an event to every local iterator for the
trigger before `withFilter` evaluates it.

Never trust a client-provided username or tenant argument by itself. Derive
identity from the authenticated WebSocket context and compare it in the filter.

## Error and DLQ behavior

Malformed or missing-trigger messages were easy for older implementations to
silently lose. This package deliberately does not delete them. Configure an SQS
redrive policy so poison messages eventually reach a DLQ.

Attach an `observer` before migration and alert on:

- `receive` failures;
- `malformed-message`, `missing-trigger`, and `missing-receipt-handle`;
- `listener` failures;
- `delete` failures.

Observer events contain no payload or raw error. Add application-specific
correlation IDs to safe metrics separately when needed.

## Suggested rollout

1. Add unit tests with two simultaneous iterators and nonmatching filters.
2. Confirm the queue URL and redrive policy in the target environment.
3. Instantiate the new package once and add graceful shutdown.
4. Enforce one consumer during deployment.
5. Run the LocalStack and Apollo/`graphql-ws` test suites.
6. Deploy to an integration environment with observer metrics.
7. Compare canonical job completion with subscription notifications.
8. Roll out gradually with an immutable rollback version.

The database or source service remains authoritative throughout the migration.
SQS and WebSockets are notification paths, not reconnect replay storage.
