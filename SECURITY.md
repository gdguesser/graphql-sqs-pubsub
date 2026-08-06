# Security policy

## Supported versions

Before 1.0, only the latest published minor release receives security fixes.
After 1.0, support ranges will be documented here.

## Reporting a vulnerability

Use GitHub's private security-advisory form for this repository. Do not disclose
an unpatched vulnerability in a public issue, discussion, pull request, or
observer log.

Include:

- affected version and Node.js version;
- a minimal reproduction without real credentials or production payloads;
- the expected and observed impact;
- whether standard/FIFO queues or lifecycle behavior are involved;
- any suggested mitigation.

Maintainers should acknowledge a report within five business days and coordinate
disclosure after a fix is available. This is a target, not a service-level
agreement.

## Operational security

- Supply credentials through the AWS SDK default provider chain or an injected
  client. Never commit access keys.
- Grant only `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and/or `sqs:SendMessage`
  required by the process.
- Restrict the IAM resource to the intended queue ARN.
- Encrypt queues and DLQs according to the application's data classification.
- Treat message bodies as application data. The package never places them in
  observer events.
- Keep raw listener errors out of shared telemetry when they can contain PII.
- Authenticate WebSocket connections and derive tenant/user identity server-side
  before applying subscription filters.
- Query canonical state after reconnect; do not treat a WebSocket notification
  as authorization or durable proof of state.

## Dependency and release security

The repository includes production dependency audit, Dependabot, CodeQL,
dependency review, package-content validation, and npm trusted publishing with
provenance. Release workflows use short-lived OIDC identity rather than a stored
npm automation token.
