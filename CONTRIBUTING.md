# Contributing

Thank you for helping improve `@gdguesser/graphql-sqs-pubsub`.

## Requirements

- Node.js 22 or 24
- npm
- Docker only for the opt-in LocalStack test

## Setup

```sh
npm ci
npm test
```

Run the complete local quality gate before opening a pull request:

```sh
npm run check
```

This checks formatting, lint, strict types, coverage thresholds, both package
formats, packed contents, and production dependency audit results.

## Integration test

```sh
npm run localstack:up
npm run test:integration
npm run localstack:down
```

The test creates and deletes a temporary queue inside LocalStack. Do not point
it at an AWS account.

## Change guidelines

- Add a regression test before or with every behavior fix.
- Keep one broker worker per instance and preserve per-listener unsubscribe.
- Do not delete malformed, missing-trigger, or locally undispatched messages.
- Keep observer events free of payloads, message bodies, receipt handles, raw
  errors, usernames, and other PII.
- Do not add hidden queue, topic, policy, or IAM provisioning.
- Do not claim exactly-once or reconnect replay.
- Keep SNS code out of the direct-SQS release until its topology and lifecycle
  have an accepted design.
- Use only public `graphql-subscriptions` APIs.

## Pull requests

Describe:

- the behavior and reason for the change;
- delivery or deletion semantics affected;
- standard/FIFO and lifecycle implications;
- tests run, including whether LocalStack was available;
- documentation or migration impact.

Small, focused pull requests are easier to review. Update `CHANGELOG.md` for
user-visible changes.

## Releases

Maintainers release through the protected GitHub environment and npm trusted
publishing workflow. Do not add long-lived npm tokens. A release must pass CI,
CodeQL, dependency review where applicable, package-content validation, and the
production dependency audit.
