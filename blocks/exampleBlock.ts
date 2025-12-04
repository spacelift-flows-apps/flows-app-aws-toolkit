import { AppBlock, events } from "@slflows/sdk/v1";

export const exampleBlock: AppBlock = {
  name: "Example Action",
  description: "A simple example block that processes text input",
  category: "Example",

  inputs: {
    default: {
      config: {
        message: {
          name: "Message",
          description: "The text message to process",
          type: "string",
          required: true,
        },
      },
      onEvent: async (input) => {
        const message = input.event.inputConfig.message;
        const apiKey = input.app.config.apiKey;
        const baseUrl = input.app.config.baseUrl;

        // Process the message (this is where you'd call your external API)
        // Example: const response = await fetch(`${baseUrl}/process`, {
        //   headers: { 'Authorization': `Bearer ${apiKey}` },
        //   body: JSON.stringify({ message })
        // });
        const processedMessage = `Processed: ${message} (using ${baseUrl} with API key: ${apiKey.substring(0, 8)}...)`;

        await events.emit({
          message: processedMessage,
          originalMessage: message,
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Result",
      description: "The processed message result",
      default: true,
      type: {
        type: "object",
        properties: {
          message: { type: "string" },
          originalMessage: { type: "string" },
        },
        required: ["message", "originalMessage"],
      },
    },
  },
};
