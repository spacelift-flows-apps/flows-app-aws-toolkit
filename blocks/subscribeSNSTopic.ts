import { AppBlock, EntityInput, EntityOnHTTPRequestInput, EntityOnInternalMessageInput, events, messaging, lifecycle, kv } from "@slflows/sdk/v1";
import { SNSClient, SubscribeCommand, SubscribeCommandInput, UnsubscribeCommand, UnsubscribeCommandInput } from "@aws-sdk/client-sns";

const subscriptionConfirmationKey = "subscription-confirmation"

enum SubscriptionStatus {
	PENDING,
	FAILED,
	CONFIRMED
}

interface Subscription {
	arn: string,
	status: SubscriptionStatus,

	// Failed statuses may include description.
	description?: string
}

const subscriptionTimeoutSeconds = 30
const subscriptionRecheckSeconds = 5

export const subscribeSNSTopic: AppBlock = {
	name: "Subscribe to SNS topic",
	description: "Subscribes to an Amazon SNS topic and emits messages as events.",
	config: {
		region: {
			name: "Region",
			description: "AWS region where SNS topic is located.",
			type: "string",
			required: true,
		},
		topicArn: {
			name: "Topic Arn",
			description: "The ARN of the topic you want to subscribe to.",
			type: "string",
			required: true,
		},
		attributes: {
			name: "Attributes",
			description: "A map of attributes with their corresponding values.",
			type: {
				type: "object",
				additionalProperties: {
					type: "string",
				},
			},
			required: false,
		},
	},
	async onInternalMessage(input: EntityOnInternalMessageInput) {
		switch (input.message.body.Type) {
			case "SubscriptionConfirmation":
				const storedValue = await kv.block.get(subscriptionConfirmationKey);
				if (!storedValue.value) {
					console.warn("Received unexpected subscription confirmation request")
					return
				}

				const subscription = storedValue.value as Subscription;

				try {
					const response = await fetch(input.message.body.SubscribeURL);

					if (response.status === 200) {
						kv.block.set({
							key: subscriptionConfirmationKey,
							value: {
								arn: subscription.arn,
								status: SubscriptionStatus.CONFIRMED
							} as Subscription
						})
					} else {
						kv.block.set({
							key: subscriptionConfirmationKey,
							ttl: subscriptionTimeoutSeconds,
							value: {
								arn: subscription.arn,
								status: SubscriptionStatus.FAILED,
								description: `Rejected SNS subscription confirmation, status code: ${response.status}`
							} as Subscription
						})
					}
				} catch (error: any) {
					kv.block.set({
						key: subscriptionConfirmationKey,
						ttl: subscriptionTimeoutSeconds,
						value: {
							arn: subscription.arn,
							status: SubscriptionStatus.FAILED,
							description: `Failed to confirm SNS subscription: ${error.message}`
						} as Subscription
					})
				}

				lifecycle.sync()

				break;
			case "Notification":
				await events.emit({
					message: input.message.body
				})

				break;
			default:
				console.warn(`Unexpected SNS message type: ${input.message.body.Type}`);
		}
	},
	async onSync(input: EntityInput) {
		const storedValue = await kv.block.get(subscriptionConfirmationKey);

		if (!storedValue.value) {
			const client = new SNSClient({
				region: input.block.config.region,
				credentials: {
					accessKeyId: input.app.config.accessKeyId,
					secretAccessKey: input.app.config.secretAccessKey,
					sessionToken: input.app.config.sessionToken,
				},
				endpoint: input.app.config.endpoint,
			});

			const endpointURL = input.block.http!.url
			const isSecure = endpointURL.startsWith("https")

			const command = new SubscribeCommand({
				TopicArn: input.block.config.topicArn,
				Protocol: isSecure ? "https" : "http",
				Endpoint: endpointURL,
				Attributes: input.block.config.attributes,
				ReturnSubscriptionArn: true
			} as SubscribeCommandInput);

			const response = await client.send(command);

			if (response.$metadata.httpStatusCode !== 200) {
				const errMsg = `Couldn't issue SNS Subscribe command, statusCode: ${response.$metadata.httpStatusCode}`

				console.error(errMsg)

				return {
					newStatus: "failed",
					customStatusDescription: errMsg
				}
			}

			await kv.block.set({
				key: subscriptionConfirmationKey,
				ttl: subscriptionTimeoutSeconds,
				value: {
					arn: response.SubscriptionArn,
					status: SubscriptionStatus.PENDING
				} as Subscription
			})

			return {
				newStatus: "in_progress",
				nextScheduleDelay: subscriptionRecheckSeconds
			}
		}

		const subscription = storedValue.value as Subscription;

		switch (subscription.status) {
			case SubscriptionStatus.PENDING:
				return {
					newStatus: "in_progress",
					nextScheduleDelay: subscriptionRecheckSeconds
				}
			case SubscriptionStatus.FAILED:
				console.error(subscription.description)

				return {
					newStatus: "failed",
					customStatusDescription: subscription.description
				}
		}

		return {
			newStatus: "ready"
		}
	},
	async onDrain(input: EntityInput) {
		const storedValue = await kv.block.get(subscriptionConfirmationKey);

		if (storedValue.value) {
			const subscription = storedValue.value as Subscription;

			const client = new SNSClient({
				region: input.block.config.region,
				credentials: {
					accessKeyId: input.app.config.accessKeyId,
					secretAccessKey: input.app.config.secretAccessKey,
					sessionToken: input.app.config.sessionToken,
				},
				endpoint: input.app.config.endpoint,
			});

			const command = new UnsubscribeCommand({
				SubscriptionArn: subscription.arn,
			} as UnsubscribeCommandInput)

			const response = await client.send(command);

			if (response.$metadata.httpStatusCode !== 200) {
				const errMsg = `Couldn't issue SNS Unsubscribe command, statusCode: ${response.$metadata.httpStatusCode}`

				console.error(errMsg)

				return {
					newStatus: "draining_failed",
					customStatusDescription: errMsg
				}
			}

			await kv.block.delete([subscriptionConfirmationKey])
		}

		return {
			newStatus: "drained"
		}
	},
	http: {
		async onRequest(input: EntityOnHTTPRequestInput) {
			// Forward requests to internal message handler.
			messaging.sendToBlocks({
				body: input.request.body,
				blockIds: [input.block.id]
			})
		},
	},
	outputs: {
		default: {
			name: "On Message",
			description: "Emitted SNS message payload",
			type: {
				type: "object",
				properties: {
					message: {
						type: "string",
						description: 'Incoming SNS Topic message.',
					},
				},
				required: ["message"]
			},
		},
	},
};
