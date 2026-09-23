import type { RegistryEntry } from "../../shared.ts";

export const outlier_aiProvider: RegistryEntry = {
  id: "outlier-ai",
  alias: "outlier",
  format: "openai",
  executor: "outlier-ai",
  baseUrl: "https://playground.outlier.ai/internal/experts/assistant",
  authType: "apikey",
  authHeader: "cookie",
  passthroughModels: true,
  models: [
    {
      id: "outlier-playground",
      name: "Outlier Playground (Auto)",
    },
  ],
};
