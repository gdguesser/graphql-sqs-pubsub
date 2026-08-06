import { createClient, type Client } from "graphql-ws";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WebSocket } from "ws";

import { startExampleServer } from "../../examples/apollo-server";
import { SQSPubSub, type SQSClientLike } from "../../src";
import { FakeSqsClient } from "../helpers/fake-sqs-client";

interface MessageResult {
  readonly message: {
    readonly room: string;
    readonly text: string;
  };
}

const clients: Client[] = [];
const stops: (() => Promise<void>)[] = [];

afterEach(async () => {
  await Promise.all(clients.splice(0).map(async (client) => client.dispose()));
  await Promise.all(stops.splice(0).map(async (stop) => stop()));
});

describe("Apollo Server 5 and graphql-ws example", () => {
  it("fans out to two WebSocket clients and filters by arguments", async () => {
    const sqs = new FakeSqsClient({ loopbackPublishes: true });
    const pubsub = new SQSPubSub({
      queueUrl: "https://sqs.us-east-1.amazonaws.com/123/example-events",
      client: sqs as unknown as SQSClientLike,
    });
    const server = await startExampleServer({ pubsub });
    stops.push(server.stop);

    const roomAClient = createClient({
      url: server.wsUrl,
      webSocketImpl: WebSocket,
    });
    const roomBClient = createClient({
      url: server.wsUrl,
      webSocketImpl: WebSocket,
    });
    clients.push(roomAClient, roomBClient);

    const query = `
      subscription Message($room: String!) {
        message(room: $room) {
          room
          text
        }
      }
    `;
    const roomAIterator = roomAClient.iterate<MessageResult>({
      query,
      variables: { room: "room-a" },
    });
    const roomBIterator = roomBClient.iterate<MessageResult>({
      query,
      variables: { room: "room-b" },
    });
    const roomANext = roomAIterator.next();
    const roomBNext = roomBIterator.next();

    await vi.waitFor(() => {
      expect(pubsub.getSubscriberStats("MESSAGE").listenerCount).toBe(2);
    });

    await publish(server.httpUrl, "room-a", "hello a");
    await expect(roomANext).resolves.toMatchObject({
      done: false,
      value: {
        data: {
          message: {
            room: "room-a",
            text: "hello a",
          },
        },
      },
    });

    await publish(server.httpUrl, "room-b", "hello b");
    await expect(roomBNext).resolves.toMatchObject({
      done: false,
      value: {
        data: {
          message: {
            room: "room-b",
            text: "hello b",
          },
        },
      },
    });

    await roomAIterator.return?.();
    await roomBIterator.return?.();
  });
});

async function publish(
  httpUrl: string,
  room: string,
  text: string,
): Promise<void> {
  const response = await fetch(httpUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      query: `
        mutation Publish($room: String!, $text: String!) {
          publishMessage(room: $room, text: $text)
        }
      `,
      variables: { room, text },
    }),
  });

  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toEqual({
    data: { publishMessage: true },
  });
}
