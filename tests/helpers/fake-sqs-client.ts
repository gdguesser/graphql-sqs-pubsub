import {
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

interface SendOptions {
  readonly abortSignal?: AbortSignal;
}

interface PendingReceive {
  resolve(messages: Message[]): void;
  reject(error: Error): void;
}

export class FakeSqsClient {
  public readonly commands: unknown[] = [];
  public readonly receiveSignals: AbortSignal[] = [];
  public activeReceives = 0;
  public maxActiveReceives = 0;
  public destroyed = false;

  readonly #receiveBatches: Message[][] = [];
  readonly #receiveErrors: Error[] = [];
  readonly #deleteErrors: Error[] = [];
  readonly #sendErrors: Error[] = [];
  readonly #pendingReceives: PendingReceive[] = [];
  readonly #loopbackPublishes: boolean;
  #published = 0;

  public constructor(options: { loopbackPublishes?: boolean } = {}) {
    this.#loopbackPublishes = options.loopbackPublishes ?? false;
  }

  public enqueue(...messages: Message[]): void {
    const pending = this.#pendingReceives.shift();
    if (pending !== undefined) {
      pending.resolve(messages);
      return;
    }
    this.#receiveBatches.push(messages);
  }

  public failNextReceive(error = new Error("receive failed")): void {
    this.#receiveErrors.push(error);
  }

  public failNextDelete(error = new Error("delete failed")): void {
    this.#deleteErrors.push(error);
  }

  public failNextSend(error = new Error("send failed")): void {
    this.#sendErrors.push(error);
  }

  public async send(
    command: unknown,
    options: SendOptions = {},
  ): Promise<unknown> {
    this.commands.push(command);

    if (command instanceof ReceiveMessageCommand) {
      return this.#receive(options.abortSignal);
    }

    if (command instanceof DeleteMessageCommand) {
      const error = this.#deleteErrors.shift();
      if (error !== undefined) {
        throw error;
      }
      return {};
    }

    if (command instanceof SendMessageCommand) {
      const error = this.#sendErrors.shift();
      if (error !== undefined) {
        throw error;
      }
      this.#published += 1;
      const messageId = `published-${this.#published}`;
      if (this.#loopbackPublishes) {
        this.enqueue({
          MessageId: messageId,
          ReceiptHandle: `receipt-${this.#published}`,
          Body: command.input.MessageBody,
          MessageAttributes: command.input.MessageAttributes,
        });
      }
      return { MessageId: messageId };
    }

    throw new Error("Unexpected SQS command");
  }

  public destroy(): void {
    this.destroyed = true;
  }

  async #receive(signal: AbortSignal | undefined): Promise<unknown> {
    this.activeReceives += 1;
    this.maxActiveReceives = Math.max(
      this.maxActiveReceives,
      this.activeReceives,
    );
    if (signal !== undefined) {
      this.receiveSignals.push(signal);
    }

    try {
      const error = this.#receiveErrors.shift();
      if (error !== undefined) {
        throw error;
      }

      const messages = this.#receiveBatches.shift();
      if (messages !== undefined) {
        return { Messages: messages };
      }

      return await new Promise((resolve, reject) => {
        const onAbort = (): void => {
          cleanup();
          const abortError = new Error("The operation was aborted");
          abortError.name = "AbortError";
          reject(abortError);
        };
        const cleanup = (): void => {
          signal?.removeEventListener("abort", onAbort);
          const index = this.#pendingReceives.indexOf(pending);
          if (index >= 0) {
            this.#pendingReceives.splice(index, 1);
          }
        };
        const pending: PendingReceive = {
          resolve: (nextMessages) => {
            cleanup();
            resolve({ Messages: nextMessages });
          },
          reject: (nextError) => {
            cleanup();
            reject(nextError);
          },
        };

        if (signal?.aborted === true) {
          onAbort();
          return;
        }

        this.#pendingReceives.push(pending);
        signal?.addEventListener("abort", onAbort, { once: true });
      });
    } finally {
      this.activeReceives -= 1;
    }
  }
}

export function sqsMessage(
  trigger: string,
  payload: unknown,
  sequence = 1,
): Message {
  return {
    MessageId: `message-${sequence}`,
    ReceiptHandle: `receipt-${sequence}`,
    Body: JSON.stringify(payload),
    MessageAttributes: {
      SQSPubSubTriggerName: {
        DataType: "String",
        StringValue: trigger,
      },
    },
  };
}
