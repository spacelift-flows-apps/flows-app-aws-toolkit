import { defineApp } from "@slflows/sdk/v1";
import { blocks } from "./blocks/index";

export const app = defineApp({
  name: "{{APP_NAME}}",
  installationInstructions:
    "{{APP_DESCRIPTION}}\n\nTo install:\n1. Add your API key\n2. Configure the base URL if needed\n3. Start using the blocks in your flows",

  blocks,

  config: {
    apiKey: {
      name: "API Key",
      description: "Your service API key",
      type: "string",
      required: true,
      sensitive: true,
    },
    baseUrl: {
      name: "Base URL",
      description: "API base URL",
      type: "string",
      required: false,
      default: "https://api.example.com",
    },
  },
});
