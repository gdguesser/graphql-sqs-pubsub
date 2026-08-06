import { createServer, type Server as HttpServer } from "node:http";
import { pathToFileURL } from "node:url";

import { ApolloServer } from "@apollo/server";
import { expressMiddleware } from "@as-integrations/express5";
import { makeExecutableSchema } from "@graphql-tools/schema";
import express from "express";
import { withFilter } from "graphql-subscriptions";
import { useServer } from "graphql-ws/use/ws";
import { WebSocketServer } from "ws";

import { SQSPubSub, type SQSPubSubOptions } from "../src";

const MESSAGE_TRIGGER = "MESSAGE";

interface Message {
  readonly room: string;
  readonly text: string;
}

interface MessagePayload {
  readonly message: Message;
}

interface RoomArguments {
  readonly room: string;
}

interface PublishArguments extends RoomArguments {
  readonly text: string;
}

interface GraphQLContext {
  readonly pubsub: SQSPubSub;
}

export interface ExampleServerOptions {
  readonly port?: number;
  readonly pubsub?: SQSPubSub;
  readonly sqs?: SQSPubSubOptions;
}

export interface RunningExampleServer {
  readonly httpUrl: string;
  readonly wsUrl: string;
  readonly pubsub: SQSPubSub;
  stop(): Promise<void>;
}

const typeDefs = `#graphql
  type Message {
    room: String!
    text: String!
  }

  type Query {
    health: String!
  }

  type Mutation {
    publishMessage(room: String!, text: String!): Boolean!
  }

  type Subscription {
    message(room: String!): Message!
  }
`;

const resolvers = {
  Query: {
    health: (): string => "ok",
  },
  Mutation: {
    publishMessage: async (
      _root: unknown,
      arguments_: PublishArguments,
      context: GraphQLContext,
    ): Promise<boolean> => {
      await context.pubsub.publish(MESSAGE_TRIGGER, {
        message: {
          room: arguments_.room,
          text: arguments_.text,
        },
      } satisfies MessagePayload);
      return true;
    },
  },
  Subscription: {
    message: {
      subscribe: withFilter<MessagePayload, RoomArguments, GraphQLContext>(
        (_root, _arguments, context) => {
          if (context === undefined) {
            throw new Error("Missing GraphQL context");
          }
          return context.pubsub.asyncIterableIterator<MessagePayload>(
            MESSAGE_TRIGGER,
          );
        },
        (payload, arguments_) => payload?.message.room === arguments_?.room,
      ),
    },
  },
};

export async function startExampleServer(
  options: ExampleServerOptions = {},
): Promise<RunningExampleServer> {
  const pubsub = options.pubsub ?? createPubSubFromEnvironment(options.sqs);
  const schema = makeExecutableSchema({ typeDefs, resolvers });
  const app = express();
  const httpServer = createServer(app);
  const webSocketServer = new WebSocketServer({
    server: httpServer,
    path: "/graphql",
  });
  const webSocketCleanup = useServer(
    {
      schema,
      context: async () => ({ pubsub }),
    },
    webSocketServer,
  );

  const apollo = new ApolloServer<GraphQLContext>({
    schema,
    plugins: [
      {
        async serverWillStart() {
          return {
            async drainServer() {
              await webSocketCleanup.dispose();
              await pubsub.close();
            },
          };
        },
      },
    ],
  });

  await apollo.start();
  app.use(
    "/graphql",
    express.json(),
    expressMiddleware(apollo, {
      context: async () => ({ pubsub }),
    }),
  );

  await listen(httpServer, options.port ?? 0);
  const address = httpServer.address();
  if (address === null || typeof address === "string") {
    throw new Error("The example server did not bind a TCP port");
  }

  const httpUrl = `http://127.0.0.1:${address.port}/graphql`;
  const wsUrl = `ws://127.0.0.1:${address.port}/graphql`;
  let stopped = false;

  return {
    httpUrl,
    wsUrl,
    pubsub,
    async stop() {
      if (stopped) {
        return;
      }
      stopped = true;
      await apollo.stop();
      await closeHttpServer(httpServer);
    },
  };
}

function createPubSubFromEnvironment(
  options: SQSPubSubOptions | undefined,
): SQSPubSub {
  if (options !== undefined) {
    return new SQSPubSub(options);
  }

  const queueUrl = process.env.SQS_QUEUE_URL;
  if (queueUrl === undefined || queueUrl.trim().length === 0) {
    throw new Error(
      "Set SQS_QUEUE_URL or pass an SQSPubSub instance to startExampleServer",
    );
  }

  return new SQSPubSub({
    queueUrl,
    clientConfig: {
      region: process.env.AWS_REGION ?? "us-east-1",
    },
  });
}

async function listen(server: HttpServer, port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

async function closeHttpServer(server: HttpServer): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

const entryPoint = process.argv[1];
if (
  entryPoint !== undefined &&
  import.meta.url === pathToFileURL(entryPoint).href
) {
  const running = await startExampleServer({ port: 4000 });
  process.stdout.write(`HTTP ${running.httpUrl}\nWebSocket ${running.wsUrl}\n`);
}
