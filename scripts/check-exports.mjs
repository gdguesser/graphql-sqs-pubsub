import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const esm = await import("@gdguesser/graphql-sqs-pubsub");
const commonjs = require("@gdguesser/graphql-sqs-pubsub");

for (const loaded of [esm, commonjs]) {
  if (typeof loaded.SQSPubSub !== "function") {
    throw new Error("SQSPubSub is missing from a package export");
  }
  if (typeof loaded.GraphqlSqsPubSubError !== "function") {
    throw new Error("GraphqlSqsPubSubError is missing from a package export");
  }
  if (loaded.DEFAULT_TRIGGER_ATTRIBUTE !== "SQSPubSubTriggerName") {
    throw new Error("The trigger constant has an unexpected export value");
  }
}

process.stdout.write("Validated ESM and CommonJS package exports.\n");
