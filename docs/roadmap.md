# Roadmap

This document separates shipped behavior from possible future work. It is not a
compatibility promise.

## Shipped in 0.1

- direct SQS only;
- one consuming process and one worker per `SQSPubSub` instance;
- local trigger fan-out with per-listener IDs;
- AWS SDK v3 injection and default client construction;
- explicit existing queue URL and no resource provisioning;
- standard and FIFO publishing;
- abortable lifecycle and payload-free observer events;
- unit, LocalStack, Apollo Server 5, and `graphql-ws` coverage.

There is no SNS client, SNS topic support, topic-to-queue subscription
management, per-replica queue discovery, or multi-replica broadcast in 0.1.

## Before 1.0

- gather production feedback on lifecycle and observer event names;
- test additional supported GraphQL 16 patch releases and Node 24;
- evaluate bounded iterator queues and explicit overflow policies;
- document visibility-timeout sizing from measured listener latency;
- stabilize the internal transport contract or keep it private;
- add compatibility fixtures from representative legacy producers;
- publish release candidates before declaring the API stable.

## Optional SNS / per-replica-SQS transport

The AWS-native multi-replica topology under consideration is:

```text
producer
   |
   v
SNS topic
   |
   +----> SQS queue for consumer A ----> GraphQL server A ----> its clients
   |
   +----> SQS queue for consumer B ----> GraphQL server B ----> its clients
   |
   +----> SQS queue for consumer C ----> GraphQL server C ----> its clients
```

SNS performs inter-process broadcast. Each server still uses one local SQS
worker and the same in-process listener registry.

Design work must resolve:

- stable queue identity and cleanup during autoscaling and crashes;
- queue and SNS subscription policies;
- raw SNS delivery versus SNS envelope parsing;
- filter policies and their effect on routing;
- deployment overlap and queue handoff;
- FIFO topic/queue compatibility and message group strategy;
- redrive and DLQ ownership for each replica queue;
- who provisions resources and when.

Production resource creation should remain external and explicit. A future
transport may accept pre-provisioned topic and queue identifiers or be paired
with documented Terraform/CDK modules. The runtime package should not silently
mutate IAM or create durable resources.

The private `MessageTransport` seam already separates receive, acknowledge,
publish, and close operations from local fan-out. It exists to make this future
work possible without adding dormant SNS code to the initial release.

## Other possible work

- separate publisher and subscriber clients for least-privilege deployments;
- OpenTelemetry-oriented observer adapters in separate optional packages;
- pluggable bounded buffering and slow-consumer policies;
- richer health/readiness state without performing queue mutations;
- conformance tests for custom transport implementations if the seam becomes
  public.

## Non-goals

- end-to-end exactly-once WebSocket delivery;
- replaying events to clients that were offline;
- using SQS as canonical application state;
- provisioning hidden production infrastructure;
- implementing a general event-stream platform;
- replacing managed AppSync subscriptions or AppSync Events when those are the
  better architectural fit.

Clients should continue to query canonical state on initial connection and
reconnection, regardless of future transport additions.
