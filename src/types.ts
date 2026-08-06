import type { SQSClient, SQSClientConfig } from "@aws-sdk/client-sqs";

export const DEFAULT_TRIGGER_ATTRIBUTE = "SQSPubSubTriggerName";

export type QueueType = "auto" | "standard" | "fifo";

export type LifecycleState = "idle" | "running" | "closing" | "closed";

export type ObserverPhase =
  | "lifecycle"
  | "receive"
  | "validate"
  | "dispatch"
  | "listener"
  | "delete"
  | "publish";

export type ObserverOutcome = "started" | "succeeded" | "failed" | "skipped";

export type ErrorCategory =
  | "receive"
  | "malformed-message"
  | "missing-trigger"
  | "missing-receipt-handle"
  | "listener"
  | "delete"
  | "serialization"
  | "publish"
  | "lifecycle";

/**
 * Deliberately excludes message bodies, payloads, receipt handles, and raw errors.
 * Trigger names are included and should be static identifiers, never user data.
 */
export interface ObserverEvent {
  readonly phase: ObserverPhase;
  readonly outcome: ObserverOutcome;
  readonly errorCategory?: ErrorCategory;
  readonly messageId?: string;
  readonly trigger?: string;
  readonly listenerCount?: number;
  readonly messageCount?: number;
  readonly subscriptionId?: number;
}

export type Observer = (event: ObserverEvent) => void;

export type SQSClientLike = Pick<SQSClient, "send"> & {
  destroy?: () => void;
};

export interface FifoDefaults {
  /**
   * Defaults to the trigger name when omitted.
   */
  readonly messageGroupId?: string;
  /**
   * Generates a value per publish. Defaults to a random UUID.
   */
  readonly messageDeduplicationId?: (trigger: string) => string;
}

export interface PublishOptions {
  readonly messageGroupId?: string;
  readonly messageDeduplicationId?: string;
}

export interface SQSPubSubOptions {
  /**
   * Existing queue URL. This package never creates production resources.
   */
  readonly queueUrl: string;
  /**
   * Inject an AWS SDK v3-compatible SQS client.
   */
  readonly client?: SQSClientLike;
  /**
   * Used to construct the default SQSClient when client is omitted.
   */
  readonly clientConfig?: SQSClientConfig;
  /**
   * Injected clients are not destroyed by default. Package-created clients are.
   */
  readonly destroyClientOnClose?: boolean;
  readonly autoStart?: boolean;
  readonly triggerAttribute?: string;
  readonly waitTimeSeconds?: number;
  readonly maxNumberOfMessages?: number;
  readonly visibilityTimeout?: number;
  readonly receiveErrorBackoffMs?: number;
  /**
   * "auto" recognizes queue URLs whose queue name ends in ".fifo"; all other
   * queue URLs use standard publishing unless explicitly set to "fifo".
   */
  readonly queueType?: QueueType;
  readonly fifo?: FifoDefaults;
  readonly observer?: Observer;
}

export interface SubscriberStats {
  readonly state: LifecycleState;
  readonly listenerCount: number;
}
