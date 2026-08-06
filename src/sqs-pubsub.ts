import { SQSClient } from "@aws-sdk/client-sqs";
import { PubSubEngine } from "graphql-subscriptions";

import { GraphqlSqsPubSubError } from "./errors";
import { queueUrlLooksFifo, SqsTransport } from "./sqs-transport";
import type { MessageTransport, TransportMessage } from "./transport";
import {
  DEFAULT_TRIGGER_ATTRIBUTE,
  type LifecycleState,
  type ObserverEvent,
  type PublishOptions,
  type QueueType,
  type SQSPubSubOptions,
  type SubscriberStats,
} from "./types";

type Listener = (payload: unknown) => unknown;

const DEFAULT_WAIT_TIME_SECONDS = 20;
const DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;
const DEFAULT_RECEIVE_ERROR_BACKOFF_MS = 1_000;

export class SQSPubSub extends PubSubEngine {
  readonly #transport: MessageTransport;
  readonly #observer: SQSPubSubOptions["observer"];
  readonly #receiveErrorBackoffMs: number;
  readonly #listenersByTrigger = new Map<string, Map<number, Listener>>();
  readonly #triggerBySubscription = new Map<number, string>();

  #nextSubscriptionId = 1;
  #state: LifecycleState = "idle";
  #abortController: AbortController | undefined;
  #worker: Promise<void> | undefined;
  #closePromise: Promise<void> | undefined;

  public constructor(options: SQSPubSubOptions) {
    super();

    const queueUrl = requireNonEmpty("queueUrl", options.queueUrl);
    const triggerAttribute = requireNonEmpty(
      "triggerAttribute",
      options.triggerAttribute ?? DEFAULT_TRIGGER_ATTRIBUTE,
    );
    const waitTimeSeconds = integerInRange(
      "waitTimeSeconds",
      options.waitTimeSeconds ?? DEFAULT_WAIT_TIME_SECONDS,
      0,
      20,
    );
    const maxNumberOfMessages = integerInRange(
      "maxNumberOfMessages",
      options.maxNumberOfMessages ?? DEFAULT_MAX_NUMBER_OF_MESSAGES,
      1,
      10,
    );
    const visibilityTimeout =
      options.visibilityTimeout === undefined
        ? undefined
        : integerInRange(
            "visibilityTimeout",
            options.visibilityTimeout,
            0,
            43_200,
          );

    this.#receiveErrorBackoffMs = integerInRange(
      "receiveErrorBackoffMs",
      options.receiveErrorBackoffMs ?? DEFAULT_RECEIVE_ERROR_BACKOFF_MS,
      0,
      60_000,
    );

    if (options.client !== undefined && options.clientConfig !== undefined) {
      throw new TypeError("Provide either client or clientConfig, not both");
    }

    const queueType = requireQueueType(options.queueType);
    const detectedFifo = queueUrlLooksFifo(queueUrl);
    if (queueType === "standard" && detectedFifo) {
      throw new TypeError(
        "A .fifo queue URL cannot be forced to standard mode",
      );
    }

    const fifoEnabled =
      queueType === "fifo" || (queueType === "auto" && detectedFifo);
    if (!fifoEnabled && options.fifo !== undefined) {
      throw new TypeError(
        "fifo defaults require a .fifo queue URL or queueType set to fifo",
      );
    }
    const ownsClient = options.client === undefined;
    const client = options.client ?? new SQSClient(options.clientConfig ?? {});

    this.#transport = new SqsTransport({
      queueUrl,
      client,
      destroyClientOnClose: options.destroyClientOnClose ?? ownsClient,
      triggerAttribute,
      waitTimeSeconds,
      maxNumberOfMessages,
      visibilityTimeout,
      fifo: fifoEnabled ? (options.fifo ?? {}) : undefined,
    });
    this.#observer = options.observer;

    if (options.autoStart ?? true) {
      this.start();
    }
  }

  public get state(): LifecycleState {
    return this.#state;
  }

  public getSubscriberStats(trigger?: string): SubscriberStats {
    if (trigger !== undefined) {
      return {
        state: this.#state,
        listenerCount: this.#listenersByTrigger.get(trigger)?.size ?? 0,
      };
    }

    let listenerCount = 0;
    for (const listeners of this.#listenersByTrigger.values()) {
      listenerCount += listeners.size;
    }
    return { state: this.#state, listenerCount };
  }

  public start(): void {
    if (this.#state === "running") {
      return;
    }
    if (this.#state === "closing" || this.#state === "closed") {
      throw new GraphqlSqsPubSubError(
        "A closed SQSPubSub instance cannot be restarted",
        "lifecycle",
      );
    }

    this.#abortController = new AbortController();
    this.#state = "running";
    this.#observe({ phase: "lifecycle", outcome: "started" });
    this.#worker = this.#runWorker(this.#abortController.signal);
  }

  public async close(): Promise<void> {
    if (this.#state === "closed") {
      return;
    }
    if (this.#closePromise !== undefined) {
      return this.#closePromise;
    }

    this.#state = "closing";
    this.#abortController?.abort();
    const worker = this.#worker;

    this.#closePromise = (async () => {
      await worker;
      this.#listenersByTrigger.clear();
      this.#triggerBySubscription.clear();
      this.#transport.close();
      this.#state = "closed";
      this.#observe({ phase: "lifecycle", outcome: "succeeded" });
    })();

    return this.#closePromise;
  }

  public override async publish(
    triggerName: string,
    payload: unknown,
    options: PublishOptions = {},
  ): Promise<void> {
    const trigger = requireNonEmpty("triggerName", triggerName);
    if (this.#state === "closing" || this.#state === "closed") {
      throw new GraphqlSqsPubSubError(
        "Cannot publish after SQSPubSub has started closing",
        "lifecycle",
      );
    }

    let body: string;
    try {
      const serialized = JSON.stringify(payload);
      if (serialized === undefined) {
        throw new TypeError("The payload is not JSON serializable");
      }
      body = serialized;
    } catch (cause) {
      this.#observe({
        phase: "publish",
        outcome: "failed",
        errorCategory: "serialization",
        trigger,
      });
      throw new GraphqlSqsPubSubError(
        "The publish payload could not be serialized as JSON",
        "serialization",
        { cause },
      );
    }

    try {
      const result = await this.#transport.publish(trigger, body, options);
      this.#observe({
        phase: "publish",
        outcome: "succeeded",
        trigger,
        ...(result.messageId === undefined
          ? {}
          : { messageId: result.messageId }),
      });
    } catch (cause) {
      this.#observe({
        phase: "publish",
        outcome: "failed",
        errorCategory: "publish",
        trigger,
      });
      if (cause instanceof GraphqlSqsPubSubError) {
        throw cause;
      }
      throw new GraphqlSqsPubSubError(
        "SQS rejected the publish operation",
        "publish",
        { cause },
      );
    }
  }

  public override async subscribe(
    triggerName: string,
    // graphql-subscriptions@3 declares this abstract parameter as Function.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    onMessage: Function,
    // graphql-subscriptions@3 declares this abstract parameter as Object.
    // eslint-disable-next-line @typescript-eslint/no-wrapper-object-types
    _options: Object = {},
  ): Promise<number> {
    const trigger = requireNonEmpty("triggerName", triggerName);
    if (typeof onMessage !== "function") {
      throw new TypeError("onMessage must be a function");
    }
    if (this.#state === "closing" || this.#state === "closed") {
      throw new GraphqlSqsPubSubError(
        "Cannot subscribe after SQSPubSub has started closing",
        "lifecycle",
      );
    }

    const subscriptionId = this.#nextSubscriptionId;
    this.#nextSubscriptionId += 1;

    const listeners =
      this.#listenersByTrigger.get(trigger) ?? new Map<number, Listener>();
    listeners.set(subscriptionId, (payload) => onMessage(payload));
    this.#listenersByTrigger.set(trigger, listeners);
    this.#triggerBySubscription.set(subscriptionId, trigger);

    return subscriptionId;
  }

  public override unsubscribe(subscriptionId: number): void {
    const trigger = this.#triggerBySubscription.get(subscriptionId);
    if (trigger === undefined) {
      return;
    }

    const listeners = this.#listenersByTrigger.get(trigger);
    listeners?.delete(subscriptionId);
    if (listeners?.size === 0) {
      this.#listenersByTrigger.delete(trigger);
    }
    this.#triggerBySubscription.delete(subscriptionId);
  }

  /**
   * Compatibility alias for graphql-subscriptions v2 and older adapters.
   */
  public asyncIterator<T>(
    triggers: string | readonly string[],
  ): AsyncIterableIterator<T> {
    return this.asyncIterableIterator<T>(triggers);
  }

  async #runWorker(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let messages: readonly TransportMessage[];
      try {
        messages = await this.#transport.receive(signal);
      } catch {
        if (signal.aborted) {
          break;
        }
        this.#observe({
          phase: "receive",
          outcome: "failed",
          errorCategory: "receive",
        });
        await abortableDelay(this.#receiveErrorBackoffMs, signal);
        continue;
      }

      if (messages.length > 0) {
        this.#observe({
          phase: "receive",
          outcome: "succeeded",
          messageCount: messages.length,
        });
      }

      for (const message of messages) {
        await this.#processMessage(message);
      }
    }
  }

  async #processMessage(message: TransportMessage): Promise<void> {
    if (message.receiptHandle === undefined) {
      this.#observeMessage(message, {
        phase: "validate",
        outcome: "failed",
        errorCategory: "missing-receipt-handle",
      });
      return;
    }

    const trigger = message.trigger?.trim();
    if (trigger === undefined || trigger.length === 0) {
      this.#observeMessage(message, {
        phase: "validate",
        outcome: "failed",
        errorCategory: "missing-trigger",
      });
      return;
    }

    let payload: unknown;
    try {
      if (message.body === undefined) {
        throw new TypeError("Missing SQS message body");
      }
      payload = JSON.parse(message.body) as unknown;
    } catch {
      this.#observeMessage(message, {
        phase: "validate",
        outcome: "failed",
        errorCategory: "malformed-message",
        trigger,
      });
      return;
    }

    const listeners = [
      ...(this.#listenersByTrigger.get(trigger)?.entries() ?? []),
    ];
    const results = await Promise.allSettled(
      listeners.map(async ([, listener]) => listener(payload)),
    );

    let failedListenerCount = 0;
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        failedListenerCount += 1;
        const subscriptionId = listeners[index]?.[0];
        this.#observeMessage(message, {
          phase: "listener",
          outcome: "failed",
          errorCategory: "listener",
          trigger,
          listenerCount: listeners.length,
          ...(subscriptionId === undefined ? {} : { subscriptionId }),
        });
      }
    });

    this.#observeMessage(message, {
      phase: "dispatch",
      outcome: failedListenerCount === 0 ? "succeeded" : "failed",
      ...(failedListenerCount === 0
        ? {}
        : { errorCategory: "listener" as const }),
      trigger,
      listenerCount: listeners.length,
    });

    if (failedListenerCount > 0) {
      return;
    }

    try {
      await this.#transport.acknowledge(message);
      this.#observeMessage(message, {
        phase: "delete",
        outcome: "succeeded",
        trigger,
        listenerCount: listeners.length,
      });
    } catch {
      this.#observeMessage(message, {
        phase: "delete",
        outcome: "failed",
        errorCategory: "delete",
        trigger,
        listenerCount: listeners.length,
      });
    }
  }

  #observeMessage(
    message: TransportMessage,
    event: Omit<ObserverEvent, "messageId">,
  ): void {
    this.#observe({
      ...event,
      ...(message.messageId === undefined
        ? {}
        : { messageId: message.messageId }),
    });
  }

  #observe(event: ObserverEvent): void {
    try {
      const observation: unknown = this.#observer?.(event);
      if (observation !== undefined) {
        void Promise.resolve(observation).catch(() => {
          // Asynchronous observer failures are isolated too.
        });
      }
    } catch {
      // Observability must never change broker or listener semantics.
    }
  }
}

function requireNonEmpty(name: string, value: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value.trim();
}

function integerInRange(
  name: string,
  value: number,
  minimum: number,
  maximum: number,
): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(
      `${name} must be an integer between ${minimum} and ${maximum}`,
    );
  }
  return value;
}

function requireQueueType(value: QueueType | undefined): QueueType {
  const candidate: unknown = value ?? "auto";
  if (
    candidate !== "auto" &&
    candidate !== "standard" &&
    candidate !== "fifo"
  ) {
    throw new TypeError("queueType must be auto, standard, or fifo");
  }
  return candidate;
}

async function abortableDelay(
  milliseconds: number,
  signal: AbortSignal,
): Promise<void> {
  if (milliseconds === 0 || signal.aborted) {
    return;
  }

  await new Promise<void>((resolve) => {
    let settled = false;
    const finish = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };

    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) {
      finish();
    }
  });
}
