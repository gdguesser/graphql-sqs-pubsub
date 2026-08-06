import { getEventListeners } from "node:events";

import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  GraphqlSqsPubSubError,
  SQSPubSub,
  type ObserverEvent,
  type SQSClientLike,
  type SQSPubSubOptions,
} from "../../src";
import { queueUrlLooksFifo, SqsTransport } from "../../src/sqs-transport";
import { FakeSqsClient, sqsMessage } from "../helpers/fake-sqs-client";

const instances: SQSPubSub[] = [];

afterEach(async () => {
  await Promise.all(
    instances.splice(0).map(async (instance) => instance.close()),
  );
});

describe("local fan-out and registry", () => {
  it("parses once and fans out to every listener on the trigger", async () => {
    const { client, pubsub } = setup();
    const first = vi.fn<(payload: unknown) => void>();
    const second = vi.fn<(payload: unknown) => void>();
    await pubsub.subscribe("orders", first, {});
    await pubsub.subscribe("orders", second, {});
    client.enqueue(sqsMessage("orders", { orderId: "42" }));

    pubsub.start();

    await vi.waitFor(() => {
      expect(first).toHaveBeenCalledOnce();
      expect(second).toHaveBeenCalledOnce();
      expect(deleteCommands(client)).toHaveLength(1);
    });
    expect(first.mock.calls[0]?.[0]).toBe(second.mock.calls[0]?.[0]);
  });

  it("isolates listeners registered for a different trigger", async () => {
    const { client, pubsub } = setup();
    const orderListener = vi.fn();
    const reportListener = vi.fn();
    await pubsub.subscribe("orders", orderListener, {});
    await pubsub.subscribe("reports", reportListener, {});
    client.enqueue(sqsMessage("reports", { reportId: "r-1" }));

    pubsub.start();

    await vi.waitFor(() => expect(reportListener).toHaveBeenCalledOnce());
    expect(orderListener).not.toHaveBeenCalled();
    expect(deleteCommands(client)).toHaveLength(1);
  });

  it("uses unique IDs and unsubscribes only the requested listener", async () => {
    const { client, pubsub } = setup();
    const first = vi.fn();
    const second = vi.fn();
    const firstId = await pubsub.subscribe("orders", first, {});
    const secondId = await pubsub.subscribe("orders", second, {});
    expect(firstId).not.toBe(secondId);

    pubsub.unsubscribe(firstId);
    pubsub.unsubscribe(firstId);
    client.enqueue(sqsMessage("orders", { orderId: "42" }));
    pubsub.start();

    await vi.waitFor(() => expect(second).toHaveBeenCalledOnce());
    expect(first).not.toHaveBeenCalled();
    expect(pubsub.getSubscriberStats("orders").listenerCount).toBe(1);
  });

  it("returns a native async iterator and keeps the asyncIterator alias", async () => {
    const { client, pubsub } = setup();
    const iterator = pubsub.asyncIterator<{ value: number }>("numbers");
    expect(iterator[Symbol.asyncIterator]()).toBe(iterator);

    const next = iterator.next();
    await vi.waitFor(() => {
      expect(pubsub.getSubscriberStats("numbers").listenerCount).toBe(1);
    });
    client.enqueue(sqsMessage("numbers", { value: 7 }));
    pubsub.start();

    await expect(next).resolves.toEqual({
      done: false,
      value: { value: 7 },
    });
    await iterator.return?.();
    expect(pubsub.getSubscriberStats("numbers").listenerCount).toBe(0);
  });

  it("consumes valid live events when no listeners are registered", async () => {
    const { client, events, pubsub } = setup();
    client.enqueue(sqsMessage("unobserved", { value: true }));

    pubsub.start();

    await vi.waitFor(() => expect(deleteCommands(client)).toHaveLength(1));
    expect(events).toContainEqual(
      expect.objectContaining({
        phase: "dispatch",
        outcome: "succeeded",
        trigger: "unobserved",
        listenerCount: 0,
      }),
    );
  });
});

describe("message failure semantics", () => {
  it("isolates listener failures and leaves the message for retry", async () => {
    const { client, events, pubsub } = setup();
    const failing = vi.fn(() => {
      throw new Error("listener rejected");
    });
    const succeeding = vi.fn();
    await pubsub.subscribe("orders", failing, {});
    await pubsub.subscribe("orders", succeeding, {});
    client.enqueue(sqsMessage("orders", { orderId: "42" }));

    pubsub.start();

    await vi.waitFor(() => {
      expect(failing).toHaveBeenCalledOnce();
      expect(succeeding).toHaveBeenCalledOnce();
      expect(events).toContainEqual(
        expect.objectContaining({
          phase: "listener",
          outcome: "failed",
          errorCategory: "listener",
        }),
      );
    });
    expect(deleteCommands(client)).toHaveLength(0);
  });

  it("does not delete malformed or missing-trigger messages", async () => {
    const { client, events, pubsub } = setup();
    client.enqueue(
      {
        MessageId: "malformed",
        ReceiptHandle: "receipt-malformed",
        Body: "{not-json",
        MessageAttributes: {
          SQSPubSubTriggerName: {
            DataType: "String",
            StringValue: "orders",
          },
        },
      },
      {
        MessageId: "missing-trigger",
        ReceiptHandle: "receipt-missing-trigger",
        Body: JSON.stringify({ orderId: "42" }),
      },
      {
        MessageId: "missing-receipt",
        Body: JSON.stringify({ orderId: "43" }),
        MessageAttributes: {
          SQSPubSubTriggerName: {
            DataType: "String",
            StringValue: "orders",
          },
        },
      },
      {
        MessageId: "missing-body",
        ReceiptHandle: "receipt-missing-body",
        MessageAttributes: {
          SQSPubSubTriggerName: {
            DataType: "String",
            StringValue: "orders",
          },
        },
      },
    );

    pubsub.start();

    await vi.waitFor(() => {
      expect(events).toContainEqual(
        expect.objectContaining({
          messageId: "malformed",
          errorCategory: "malformed-message",
        }),
      );
      expect(events).toContainEqual(
        expect.objectContaining({
          messageId: "missing-trigger",
          errorCategory: "missing-trigger",
        }),
      );
      expect(events).toContainEqual(
        expect.objectContaining({
          messageId: "missing-receipt",
          errorCategory: "missing-receipt-handle",
        }),
      );
      expect(events).toContainEqual(
        expect.objectContaining({
          messageId: "missing-body",
          errorCategory: "malformed-message",
        }),
      );
    });
    expect(deleteCommands(client)).toHaveLength(0);
  });

  it("retries after receive errors without starting a second worker", async () => {
    const { client, events, pubsub } = setup();
    const listener = vi.fn();
    await pubsub.subscribe("orders", listener, {});
    client.failNextReceive();
    client.enqueue(sqsMessage("orders", { orderId: "42" }));

    pubsub.start();

    await vi.waitFor(() => expect(listener).toHaveBeenCalledOnce());
    expect(events).toContainEqual({
      phase: "receive",
      outcome: "failed",
      errorCategory: "receive",
    });
    expect(client.maxActiveReceives).toBe(1);
  });

  it("removes completed backoff listeners from the worker abort signal", async () => {
    const { client, events, pubsub } = setup({
      receiveErrorBackoffMs: 1,
    });
    client.failNextReceive();
    client.failNextReceive();
    client.failNextReceive();

    pubsub.start();

    await vi.waitFor(() => {
      expect(
        events.filter(
          (event) => event.phase === "receive" && event.outcome === "failed",
        ),
      ).toHaveLength(3);
      expect(client.activeReceives).toBe(1);
    });
    const signal = client.receiveSignals.at(-1);
    if (signal === undefined) {
      throw new Error("The worker did not expose its receive abort signal");
    }
    expect(getEventListeners(signal, "abort")).toHaveLength(1);
  });

  it("aborts a receive-error backoff during close", async () => {
    const { client, events, pubsub } = setup({
      receiveErrorBackoffMs: 60_000,
    });
    client.failNextReceive();

    pubsub.start();
    await vi.waitFor(() => {
      expect(events).toContainEqual({
        phase: "receive",
        outcome: "failed",
        errorCategory: "receive",
      });
    });

    await expect(pubsub.close()).resolves.toBeUndefined();
  });

  it("reports delete errors and redelivers duplicates", async () => {
    const { client, events, pubsub } = setup();
    const listener = vi.fn();
    await pubsub.subscribe("orders", listener, {});
    const duplicate = sqsMessage("orders", { orderId: "42" }, 9);
    client.failNextDelete();
    client.enqueue(duplicate);
    client.enqueue(duplicate);

    pubsub.start();

    await vi.waitFor(() => {
      expect(listener).toHaveBeenCalledTimes(2);
      expect(deleteCommands(client)).toHaveLength(2);
    });
    expect(events).toContainEqual(
      expect.objectContaining({
        phase: "delete",
        outcome: "failed",
        errorCategory: "delete",
        messageId: "message-9",
      }),
    );
    expect(events).toContainEqual(
      expect.objectContaining({
        phase: "delete",
        outcome: "succeeded",
        messageId: "message-9",
      }),
    );
  });

  it("uses a custom trigger message attribute", async () => {
    const { client, pubsub } = setup({
      triggerAttribute: "EventName",
    });
    const listener = vi.fn();
    await pubsub.subscribe("orders", listener, {});
    client.enqueue({
      MessageId: "custom-attribute",
      ReceiptHandle: "custom-receipt",
      Body: JSON.stringify({ orderId: "42" }),
      MessageAttributes: {
        EventName: {
          DataType: "String",
          StringValue: "orders",
        },
      },
    });

    pubsub.start();

    await vi.waitFor(() => expect(listener).toHaveBeenCalledOnce());
    const receive = receiveCommands(client)[0];
    expect(receive?.input.MessageAttributeNames).toEqual(["EventName"]);
  });
});

describe("publishing", () => {
  it("publishes to standard queues without FIFO fields", async () => {
    const { client, events, pubsub } = setup();

    await pubsub.publish("orders", { orderId: "42" });

    const send = sendCommands(client)[0];
    expect(send?.input).toMatchObject({
      QueueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
      MessageBody: JSON.stringify({ orderId: "42" }),
      MessageAttributes: {
        SQSPubSubTriggerName: {
          DataType: "String",
          StringValue: "orders",
        },
      },
    });
    expect(send?.input.MessageGroupId).toBeUndefined();
    expect(send?.input.MessageDeduplicationId).toBeUndefined();
    expect(events).toContainEqual({
      phase: "publish",
      outcome: "succeeded",
      trigger: "orders",
      messageId: "published-1",
    });
  });

  it("safely auto-detects FIFO queue URLs", async () => {
    const { client, pubsub } = setup({
      queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events.fifo",
    });

    await pubsub.publish("orders", { orderId: "42" });

    const send = sendCommands(client)[0];
    expect(send?.input.MessageGroupId).toBe("orders");
    expect(send?.input.MessageDeduplicationId).toMatch(/^[0-9a-f-]{36}$/u);
  });

  it("supports explicit FIFO defaults and per-publish overrides", async () => {
    const { client, pubsub } = setup({
      queueType: "fifo",
      fifo: {
        messageGroupId: "default-group",
        messageDeduplicationId: (trigger) => `default-${trigger}`,
      },
    });

    await pubsub.publish(
      "orders",
      { orderId: "42" },
      {
        messageGroupId: "order-42",
        messageDeduplicationId: "dedup-42",
      },
    );

    expect(sendCommands(client)[0]?.input).toMatchObject({
      MessageGroupId: "order-42",
      MessageDeduplicationId: "dedup-42",
    });
  });

  it("rejects FIFO fields on a standard queue", async () => {
    const { pubsub } = setup({ queueType: "standard" });

    await expect(
      pubsub.publish("orders", { orderId: "42" }, { messageGroupId: "orders" }),
    ).rejects.toMatchObject({
      name: "GraphqlSqsPubSubError",
      category: "publish",
    });
  });

  it("reports serialization failures without exposing payloads", async () => {
    const { events, pubsub } = setup();
    const cyclic: { self?: unknown; secret: string } = {
      secret: "private@example.com",
    };
    cyclic.self = cyclic;

    await expect(pubsub.publish("orders", cyclic)).rejects.toMatchObject({
      category: "serialization",
    });
    expect(JSON.stringify(events)).not.toContain(cyclic.secret);
  });

  it("wraps and reports SQS publish failures", async () => {
    const { client, events, pubsub } = setup();
    client.failNextSend();

    await expect(
      pubsub.publish("orders", { orderId: "42" }),
    ).rejects.toMatchObject({
      category: "publish",
    });
    expect(events).toContainEqual({
      phase: "publish",
      outcome: "failed",
      errorCategory: "publish",
      trigger: "orders",
    });
  });
});

describe("lifecycle, defaults, and observability", () => {
  it("keeps start and close idempotent and aborts the long poll", async () => {
    const { client, pubsub } = setup();

    pubsub.start();
    pubsub.start();
    await vi.waitFor(() => expect(receiveCommands(client)).toHaveLength(1));

    await Promise.all([pubsub.close(), pubsub.close()]);

    expect(client.receiveSignals).toHaveLength(1);
    expect(client.receiveSignals[0]?.aborted).toBe(true);
    expect(client.maxActiveReceives).toBe(1);
    expect(pubsub.state).toBe("closed");
    expect(() => pubsub.start()).toThrow(GraphqlSqsPubSubError);
    await expect(pubsub.subscribe("orders", vi.fn(), {})).rejects.toMatchObject(
      { category: "lifecycle" },
    );
    await expect(
      pubsub.publish("orders", { orderId: "42" }),
    ).rejects.toMatchObject({ category: "lifecycle" });
  });

  it("supports the default SQSClient configuration path", async () => {
    const pubsub = new SQSPubSub({
      queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
      clientConfig: {
        region: "eu-west-1",
        credentials: {
          accessKeyId: "test",
          secretAccessKey: "test",
        },
      },
      autoStart: false,
    });
    instances.push(pubsub);

    expect(pubsub.state).toBe("idle");
    await pubsub.close();
    expect(pubsub.state).toBe("closed");
  });

  it("does not destroy an injected client unless requested", async () => {
    const first = setup();
    await first.pubsub.close();
    expect(first.client.destroyed).toBe(false);

    const second = setup({ destroyClientOnClose: true });
    await second.pubsub.close();
    expect(second.client.destroyed).toBe(true);
  });

  it("keeps observer events free of payloads, receipts, and raw errors", async () => {
    const { client, events, pubsub } = setup();
    const secret = "private@example.com";
    client.enqueue(sqsMessage("orders", { email: secret }));

    pubsub.start();

    await vi.waitFor(() => expect(deleteCommands(client)).toHaveLength(1));
    const serializedEvents = JSON.stringify(events);
    expect(serializedEvents).not.toContain(secret);
    for (const event of events) {
      expect(event).not.toHaveProperty("payload");
      expect(event).not.toHaveProperty("body");
      expect(event).not.toHaveProperty("receiptHandle");
      expect(event).not.toHaveProperty("error");
    }
  });

  it("isolates observer failures from message processing", async () => {
    const client = new FakeSqsClient();
    const pubsub = new SQSPubSub({
      queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
      client: client as unknown as SQSClientLike,
      autoStart: false,
      observer: () => {
        throw new Error("metrics unavailable");
      },
    });
    instances.push(pubsub);
    client.enqueue(sqsMessage("orders", { orderId: "42" }));

    pubsub.start();

    await vi.waitFor(() => expect(deleteCommands(client)).toHaveLength(1));
  });

  it("isolates asynchronous observer rejections from message processing", async () => {
    const { client, pubsub } = setup({
      observer: async () => {
        throw new Error("async metrics unavailable");
      },
    });
    client.enqueue(sqsMessage("orders", { orderId: "42" }));

    pubsub.start();

    await vi.waitFor(() => expect(deleteCommands(client)).toHaveLength(1));
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    expect(pubsub.state).toBe("running");
  });

  it.each([
    [{ queueUrl: "" }, "queueUrl"],
    [{ waitTimeSeconds: 21 }, "waitTimeSeconds"],
    [{ maxNumberOfMessages: 0 }, "maxNumberOfMessages"],
    [{ visibilityTimeout: 43_201 }, "visibilityTimeout"],
    [{ receiveErrorBackoffMs: -1 }, "receiveErrorBackoffMs"],
  ] satisfies [Partial<SQSPubSubOptions>, string][])(
    "validates configuration %j",
    (overrides, expectedName) => {
      const client = new FakeSqsClient();
      expect(
        () =>
          new SQSPubSub({
            queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
            client: client as unknown as SQSClientLike,
            autoStart: false,
            ...overrides,
          }),
      ).toThrow(expectedName);
    },
  );

  it("rejects conflicting client and queue mode configuration", () => {
    const client = new FakeSqsClient();
    expect(
      () =>
        new SQSPubSub({
          queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
          client: client as unknown as SQSClientLike,
          clientConfig: { region: "eu-west-1" },
          autoStart: false,
        }),
    ).toThrow("either client or clientConfig");

    expect(
      () =>
        new SQSPubSub({
          queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events.fifo",
          client: client as unknown as SQSClientLike,
          queueType: "standard",
          autoStart: false,
        }),
    ).toThrow("cannot be forced to standard");
  });

  it("validates queue mode without inferring FIFO from defaults", () => {
    const client = new FakeSqsClient();
    expect(
      () =>
        new SQSPubSub({
          queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
          client: client as unknown as SQSClientLike,
          queueType: "invalid" as unknown as NonNullable<
            SQSPubSubOptions["queueType"]
          >,
          autoStart: false,
        }),
    ).toThrow("queueType");

    expect(
      () =>
        new SQSPubSub({
          queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
          client: client as unknown as SQSClientLike,
          fifo: { messageGroupId: "graphql-events" },
          autoStart: false,
        }),
    ).toThrow("fifo defaults require");
  });

  it("covers internal transport guards without exposing the seam", async () => {
    const client = new FakeSqsClient();
    const transport = new SqsTransport({
      queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
      client: client as unknown as SQSClientLike,
      destroyClientOnClose: true,
      triggerAttribute: "SQSPubSubTriggerName",
      waitTimeSeconds: 20,
      maxNumberOfMessages: 10,
      visibilityTimeout: undefined,
      fifo: undefined,
    });

    await expect(
      transport.acknowledge({
        messageId: "missing-receipt",
        receiptHandle: undefined,
        body: "{}",
        trigger: "orders",
      }),
    ).rejects.toMatchObject({
      category: "missing-receipt-handle",
    });
    transport.close();
    transport.close();
    expect(client.destroyed).toBe(true);
    expect(queueUrlLooksFifo("events.fifo")).toBe(true);
    expect(queueUrlLooksFifo("events")).toBe(false);
  });
});

function setup(overrides: Partial<SQSPubSubOptions> = {}): {
  client: FakeSqsClient;
  events: ObserverEvent[];
  pubsub: SQSPubSub;
} {
  const client = new FakeSqsClient();
  const events: ObserverEvent[] = [];
  const pubsub = new SQSPubSub({
    queueUrl: "https://sqs.eu-west-1.amazonaws.com/123/events",
    autoStart: false,
    receiveErrorBackoffMs: 0,
    observer: (event) => {
      events.push(event);
    },
    ...overrides,
    client: client as unknown as SQSClientLike,
  });
  instances.push(pubsub);
  return { client, events, pubsub };
}

function deleteCommands(client: FakeSqsClient): DeleteMessageCommand[] {
  return client.commands.filter(
    (command): command is DeleteMessageCommand =>
      command instanceof DeleteMessageCommand,
  );
}

function receiveCommands(client: FakeSqsClient): ReceiveMessageCommand[] {
  return client.commands.filter(
    (command): command is ReceiveMessageCommand =>
      command instanceof ReceiveMessageCommand,
  );
}

function sendCommands(client: FakeSqsClient): SendMessageCommand[] {
  return client.commands.filter(
    (command): command is SendMessageCommand =>
      command instanceof SendMessageCommand,
  );
}
