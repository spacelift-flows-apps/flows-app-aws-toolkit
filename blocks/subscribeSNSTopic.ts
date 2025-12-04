import { AppBlock, EntityInput, EntityOnHTTPRequestInput, EntityOnInternalMessageInput, events, messaging, lifecycle, kv } from "@slflows/sdk/v1";
import { ListSubscriptionsByTopicCommand, ListSubscriptionsByTopicCommandInput, SNSClient, SubscribeCommand, SubscribeCommandInput, UnsubscribeCommand, UnsubscribeCommandInput } from "@aws-sdk/client-sns";

const subscriptionConfirmationKey = "subscription-confirmation"

enum SubscriptionStatus {
	PENDING,
	FAILED,
	CONFIRMED
}

interface SubscriptionState {
	status: SubscriptionStatus
	description?: string
	createdAt?: number
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
	signals: {
		subscriptionArn: {
			name: "Subscription Arn",
			description: "The ARN of the subscription."
		},
	},
	async onInternalMessage(input: EntityOnInternalMessageInput) {
		switch (input.message.body.Type) {
			case "SubscriptionConfirmation":
				try {
					const response = await fetch(input.message.body.SubscribeURL);

					if (response.status !== 200) {
						const errMsg = `Confirming subscription, status code: ${response.status}`

						console.error(errMsg)

						kv.block.set({
							key: subscriptionConfirmationKey,
							value: {
								status: SubscriptionStatus.FAILED,
								description: errMsg
							} as SubscriptionState
						})
					}
				} catch (error: any) {
					const errMsg = `Sending confirm subscription requeste: ${error.message}`

					console.error(errMsg)

					kv.block.set({
						key: subscriptionConfirmationKey,
						value: {
							status: SubscriptionStatus.FAILED,
							description: errMsg
						} as SubscriptionState
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
		const client = new SNSClient({
			region: input.block.config.region,
			credentials: {
				accessKeyId: input.app.config.accessKeyId,
				secretAccessKey: input.app.config.secretAccessKey,
				sessionToken: input.app.config.sessionToken,
			},
			endpoint: input.app.config.endpoint,
		});

		const subscriptionArn = input.block.lifecycle?.signals?.subscriptionArn

		// We must ensure that subscription exists.
		if (subscriptionArn) {
			const subscriptionExists = await topicSubscriptionExists(client, input.block.config.topicArn, subscriptionArn)
			if (subscriptionExists) {
				await kv.block.delete([subscriptionConfirmationKey])

				return {
					newStatus: "ready"
				}
			}

			const rawSubscriptionState = await kv.block.get(subscriptionConfirmationKey)

			if (rawSubscriptionState.value) {
				const subscriptionState = rawSubscriptionState.value as SubscriptionState

				switch (subscriptionState.status) {
					case SubscriptionStatus.PENDING:
						if (!subscriptionState.createdAt) {
							// Should never happen.
							break
						}

						// In case we weren't able to receive a confirmation
						// message within a reasonable amount of time, we
						// retry creating a subscription.
						if ((Date.now() - subscriptionState.createdAt) / 1000 > subscriptionTimeoutSeconds) {
							break
						}

						return {
							newStatus: "in_progress",
							nextScheduleDelay: subscriptionRecheckSeconds
						}
					case SubscriptionStatus.FAILED:
						return {
							newStatus: "failed",
							customStatusDescription: subscriptionState.description
						}
				}
			}
		}

		const endpointURL = input.block.http!.url

		const command = new SubscribeCommand({
			TopicArn: input.block.config.topicArn,
			Protocol: endpointURL.startsWith("https") ? "https" : "http",
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
				customStatusDescription: errMsg,
				signalUpdates: {
					subscriptionArn: null
				}
			}
		}

		await kv.block.set({
			key: subscriptionConfirmationKey,
			ttl: subscriptionTimeoutSeconds,
			value: {
				status: SubscriptionStatus.PENDING,
				createdAt: Date.now(),
			} as SubscriptionState
		})

		return {
			newStatus: "in_progress",
			nextScheduleDelay: subscriptionRecheckSeconds,
			signalUpdates: {
				subscriptionArn: response.SubscriptionArn
			}
		}
	},
	async onDrain(input: EntityInput) {
		const subscriptionArn = input.block.lifecycle?.signals?.subscriptionArn

		if (subscriptionArn) {
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
				SubscriptionArn: subscriptionArn,
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

async function topicSubscriptionExists(client: SNSClient, topicArn: string, subscriptionArn: string, nextToken?: string): Promise<boolean> {
	const command = new ListSubscriptionsByTopicCommand({
		TopicArn: topicArn,
		NextToken: nextToken,
	} as ListSubscriptionsByTopicCommandInput)

	const response = await client.send(command);

	// List subscriptions doesn't return Arns for unconfirmed subscriptions.
	if (response.Subscriptions?.findIndex((v => {
		return v.SubscriptionArn === subscriptionArn
	})) !== -1) {
		return true
	}

	if (response.NextToken) {
		return topicSubscriptionExists(client, topicArn, subscriptionArn, response.NextToken)
	}

	return false
}