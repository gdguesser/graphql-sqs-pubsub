import type { ErrorCategory } from "./types";

export class GraphqlSqsPubSubError extends Error {
  public readonly category: ErrorCategory;

  public constructor(
    message: string,
    category: ErrorCategory,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "GraphqlSqsPubSubError";
    this.category = category;
  }
}
