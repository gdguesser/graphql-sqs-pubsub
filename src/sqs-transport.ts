import { randomUUID } from "node:crypto";

import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

import { GraphqlSqsPubSubError } from "./errors";
import type { FifoDefaults, PublishOptions, SQSClientLike } from "./types";
import type {
  MessageTransport,
  PublishResult,
  TransportMessage,
} from "./transport";

interface SqsTransportOptions {
  readonly queueUrl: string;
  readonly client: SQSClientLike;
  readonly destroyClientOnClose: boolean;
  readonly triggerAttribute: string;
  readonly waitTimeSeconds: number;
  readonly maxNumberOfMessages: number;
  readonly visibilityTimeout: number | undefined;
  readonly fifo: FifoDefaults | undefined;
}

export class SqsTransport implements MessageTransport {
  readonly #queueUrl: string;
  readonly #client: SQSClientLike;
  readonly #destroyClientOnClose: boolean;
  readonly #triggerAttribute: string;
  readonly #waitTimeSeconds: number;
  readonly #maxNumberOfMessages: number;
  readonly #visibilityTimeout: number | undefined;
  readonly #fifo: FifoDefaults | undefined;
  #closed = false;

  public constructor(options: SqsTransportOptions) {
    this.#queueUrl = options.queueUrl;
    this.#client = options.client;
    this.#destroyClientOnClose = options.destroyClientOnClose;
    this.#triggerAttribute = options.triggerAttribute;
    this.#waitTimeSeconds = options.waitTimeSeconds;
    this.#maxNumberOfMessages = options.maxNumberOfMessages;
    this.#visibilityTimeout = options.visibilityTimeout;
    this.#fifo = options.fifo;
  }

  public async receive(
    signal: AbortSignal,
  ): Promise<readonly TransportMessage[]> {
    const output = await this.#client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.#queueUrl,
        WaitTimeSeconds: this.#waitTimeSeconds,
        MaxNumberOfMessages: this.#maxNumberOfMessages,
        MessageAttributeNames: [this.#triggerAttribute],
        ...(this.#visibilityTimeout === undefined
          ? {}
          : { VisibilityTimeout: this.#visibilityTimeout }),
      }),
      { abortSignal: signal },
    );

    return (output.Messages ?? []).map((message) => ({
      messageId: message.MessageId,
      receiptHandle: message.ReceiptHandle,
      body: message.Body,
      trigger: message.MessageAttributes?.[this.#triggerAttribute]?.StringValue,
    }));
  }

  public async acknowledge(message: TransportMessage): Promise<void> {
    if (message.receiptHandle === undefined) {
      throw new GraphqlSqsPubSubError(
        "Cannot acknowledge an SQS message without a receipt handle",
        "missing-receipt-handle",
      );
    }

    await this.#client.send(
      new DeleteMessageCommand({
        QueueUrl: this.#queueUrl,
        ReceiptHandle: message.receiptHandle,
      }),
    );
  }

  public async publish(
    trigger: string,
    body: string,
    options: PublishOptions = {},
  ): Promise<PublishResult> {
    if (
      this.#fifo === undefined &&
      (options.messageGroupId !== undefined ||
        options.messageDeduplicationId !== undefined)
    ) {
      throw new GraphqlSqsPubSubError(
        "FIFO publish options cannot be used with a standard queue",
        "publish",
      );
    }

    const fifoFields =
      this.#fifo === undefined
        ? {}
        : {
            MessageGroupId:
              options.messageGroupId ?? this.#fifo.messageGroupId ?? trigger,
            MessageDeduplicationId:
              options.messageDeduplicationId ??
              this.#fifo.messageDeduplicationId?.(trigger) ??
              randomUUID(),
          };

    const output = await this.#client.send(
      new SendMessageCommand({
        QueueUrl: this.#queueUrl,
        MessageBody: body,
        MessageAttributes: {
          [this.#triggerAttribute]: {
            DataType: "String",
            StringValue: trigger,
          },
        },
        ...fifoFields,
      }),
    );

    return { messageId: output.MessageId };
  }

  public close(): void {
    if (this.#closed) {
      return;
    }

    this.#closed = true;
    if (this.#destroyClientOnClose) {
      this.#client.destroy?.();
    }
  }
}

export function queueUrlLooksFifo(queueUrl: string): boolean {
  try {
    const segments = new URL(queueUrl).pathname.split("/").filter(Boolean);
    return segments.at(-1)?.endsWith(".fifo") ?? false;
  } catch {
    return queueUrl.split("?", 1)[0]?.endsWith(".fifo") ?? false;
  }
}
