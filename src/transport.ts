import type { PublishOptions } from "./types";

export interface TransportMessage {
  readonly messageId: string | undefined;
  readonly receiptHandle: string | undefined;
  readonly body: string | undefined;
  readonly trigger: string | undefined;
}

export interface PublishResult {
  readonly messageId: string | undefined;
}

/**
 * Internal seam between GraphQL subscription fan-out and broker I/O.
 *
 * Version 0.1 ships only the direct-SQS implementation. A future SNS ingress
 * can implement this contract without changing iterator or registry behavior.
 */
export interface MessageTransport {
  receive(signal: AbortSignal): Promise<readonly TransportMessage[]>;
  acknowledge(message: TransportMessage): Promise<void>;
  publish(
    trigger: string,
    body: string,
    options?: PublishOptions,
  ): Promise<PublishResult>;
  close(): void;
}
