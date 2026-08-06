import {
  CreateQueueCommand,
  DeleteQueueCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { SQSPubSub } from "../../src";

const integrationEnabled = process.env.RUN_LOCALSTACK_INTEGRATION === "1";
const endpoint = process.env.LOCALSTACK_ENDPOINT ?? "http://127.0.0.1:4566";

describe.runIf(integrationEnabled)("LocalStack SQS integration", () => {
  const client = new SQSClient({
    endpoint,
    region: "us-east-1",
    credentials: {
      accessKeyId: "test",
      secretAccessKey: "test",
    },
  });
  let standardQueueUrl = "";
  let fifoQueueUrl = "";

  beforeAll(async () => {
    const queuePrefix = `graphql-sqs-pubsub-${process.pid}-${Date.now()}`;
    const standardQueue = await client.send(
      new CreateQueueCommand({
        QueueName: queuePrefix,
      }),
    );
    if (standardQueue.QueueUrl === undefined) {
      throw new Error("LocalStack did not return a standard queue URL");
    }
    standardQueueUrl = standardQueue.QueueUrl;

    const fifoQueue = await client.send(
      new CreateQueueCommand({
        QueueName: `${queuePrefix}.fifo`,
        Attributes: { FifoQueue: "true" },
      }),
    );
    if (fifoQueue.QueueUrl === undefined) {
      throw new Error("LocalStack did not return a FIFO queue URL");
    }
    fifoQueueUrl = fifoQueue.QueueUrl;
  });

  afterAll(async () => {
    for (const queueUrl of [standardQueueUrl, fifoQueueUrl]) {
      if (queueUrl.length > 0) {
        await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
      }
    }
    client.destroy();
  });

  it("publishes once and fans out through real SQS to two iterators", async () => {
    const pubsub = new SQSPubSub({
      queueUrl: standardQueueUrl,
      client,
      waitTimeSeconds: 1,
    });
    const first = pubsub.asyncIterableIterator<{
      event: { id: string };
    }>("EVENT");
    const second = pubsub.asyncIterableIterator<{
      event: { id: string };
    }>("EVENT");
    const firstNext = first.next();
    const secondNext = second.next();

    try {
      await vi.waitFor(() => {
        expect(pubsub.getSubscriberStats("EVENT").listenerCount).toBe(2);
      });
      await pubsub.publish("EVENT", { event: { id: "localstack-1" } });

      await expect(Promise.all([firstNext, secondNext])).resolves.toEqual([
        {
          done: false,
          value: { event: { id: "localstack-1" } },
        },
        {
          done: false,
          value: { event: { id: "localstack-1" } },
        },
      ]);
    } finally {
      await first.return();
      await second.return();
      await pubsub.close();
    }
  });

  it("publishes and consumes FIFO messages through real SQS", async () => {
    const pubsub = new SQSPubSub({
      queueUrl: fifoQueueUrl,
      client,
      waitTimeSeconds: 1,
    });
    const iterator = pubsub.asyncIterableIterator<{
      event: { id: string };
    }>("FIFO_EVENT");
    const next = iterator.next();

    try {
      await vi.waitFor(() => {
        expect(pubsub.getSubscriberStats("FIFO_EVENT").listenerCount).toBe(1);
      });
      await pubsub.publish(
        "FIFO_EVENT",
        { event: { id: "localstack-fifo-1" } },
        {
          messageGroupId: "integration",
          messageDeduplicationId: "localstack-fifo-1",
        },
      );

      await expect(next).resolves.toEqual({
        done: false,
        value: { event: { id: "localstack-fifo-1" } },
      });
    } finally {
      await iterator.return();
      await pubsub.close();
    }
  });
});
