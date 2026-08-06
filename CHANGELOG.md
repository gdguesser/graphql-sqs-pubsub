# Changelog

All notable changes follow
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-06

### Added

- One abortable direct-SQS receive worker per instance.
- Trigger registry with unique subscription IDs and process-local fan-out.
- AWS SDK v3 client injection and default `SQSClient` construction.
- Standard queue publishing and safe FIFO detection/options.
- Structured, payload-free observer events.
- Idempotent lifecycle and graceful long-poll cancellation.
- Native async iterables plus the `asyncIterator` compatibility alias.
- Unit, LocalStack, Apollo Server 5, and `graphql-ws` tests.
- Dual ESM/CommonJS output, declarations, release checks, and project
  documentation.

### Fixed

- Isolated rejected asynchronous observer hooks from broker processing.
- Removed completed receive-backoff listeners from the worker abort signal.
- Kept FIFO defaults from silently changing an auto-detected standard queue into
  FIFO publishing mode.

### Known limitations

- Direct SQS supports one consuming server process per queue.
- WebSocket delivery is best-effort and has no replay or exactly-once guarantee.
- SNS and per-replica SQS fan-out are roadmap work, not part of this release.
