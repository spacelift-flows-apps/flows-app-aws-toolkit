import { defineApp } from "@slflows/sdk/v1";
import { blocks } from "./blocks";

export const app = defineApp({
	name: "AWS ToolKit",
	installationInstructions: "A list of AWS utilities",
	config: {
		accessKeyId: {
			name: "AWS Access Key ID",
			description: "AWS access key identifier",
			type: "string",
			required: true,
		},
		secretAccessKey: {
			name: "AWS Secret Access Key",
			description: "AWS secret access key",
			type: "string",
			required: true,
			sensitive: true,
		},
		sessionToken: {
			name: "AWS Session Token",
			description: "AWS session token (leave empty for IAM user credentials, required for temporary STS credentials)",
			type: "string",
			required: false,
			sensitive: true,
		},
		endpoint: {
			name: "Custom Endpoint",
			description: "Optional custom endpoint URL (useful for testing or AWS-compatible services like LocalStack)",
			type: "string",
			required: false,
		},
	},
	async onSync(_input) {
		// TODO: Check that credentials are OK?

		return {
			newStatus: "ready"
		}
	},
	blocks: blocks,
});
