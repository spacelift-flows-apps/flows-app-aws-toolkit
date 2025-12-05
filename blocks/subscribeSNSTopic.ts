import {
  AppBlock,
  EntityInput,
  EntityOnHTTPRequestInput,
  events,
  lifecycle,
  kv,
  http,
  AppContext,
  EventInput,
} from "@slflows/sdk/v1";
import {
  ListSubscriptionsByTopicCommand,
  ListSubscriptionsByTopicCommandInput,
  SNSClient,
  SubscribeCommand,
  SubscribeCommandInput,
  UnsubscribeCommand,
  UnsubscribeCommandInput,
} from "@aws-sdk/client-sns";
import SnsValidator from "sns-validator";

enum SubscriptionStatus {
  PENDING,
  FAILED,
  CONFIRMED,
}

interface SubscriptionState {
  status: SubscriptionStatus;
  description?: string;
  createdAt?: number;
}

const subscriptionConfirmationKey = "subscription-confirmation";
const subscriptionResetKey = "subscription-reset";

const subscriptionTimeoutSeconds = 30;
const subscriptionRecheckSeconds = 5;

const validator = new SnsValidator();

export const subscribeSNSTopic: AppBlock = {
  name: "Subscribe to SNS topic",
  description:
    "Subscribes to an Amazon SNS topic and emits messages as events.",
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
  inputs: {
    configChange: {
      name: "Configuration Change",
      onEvent: async (input: EventInput) => {
        console.log("Config change?");

        // Configuration updated, delete the if it exists subscription
        // and trigger sync.
        const subscriptionArn = input.block.lifecycle?.signals?.subscriptionArn;
        if (subscriptionArn) {
          try {
            await deleteTopicSubscription(
              input.app,
              input.block.config.region,
              subscriptionArn,
            );
          } catch (err: any) {
            console.error(err.message);
          }
        }

        // Reset subscription to draft.
        kv.block.set({
          key: subscriptionResetKey,
          value: true,
        });

        await lifecycle.sync();
      },
    },
  },
  outputs: {
    default: {
      name: "On Message",
      description: "Emitted SNS message payload",
      type: {
        type: "object",
        properties: {
          payload: {
            type: "object",
            description: "Incoming SNS Topic message payload.",
            properties: {
              message: {
                type: "string",
                description: "Message text.",
              },
              messageId: {
                type: "string",
                description: "Unique message identifier.",
              },
              timestamp: {
                type: "string",
                description: "Time at which the message was published.",
              },
            },
            required: ["message", "messageId", "timestamp"],
          },
        },
        required: ["payload"],
      },
    },
  },
  signals: {
    subscriptionArn: {
      name: "Subscription Arn",
      description: "The ARN of the subscription.",
    },
  },
  async onSync(input: EntityInput) {
    const resetSubscription = await kv.block.get(subscriptionResetKey);
    if (resetSubscription.value) {
      kv.block.delete([subscriptionResetKey]);

      return {
        signalUpdates: {
          subscriptionArn: null,
        },
        newStatus: "draft",
      };
    }

    const client = new SNSClient({
      region: input.block.config.region,
      credentials: {
        accessKeyId: input.app.config.accessKeyId,
        secretAccessKey: input.app.config.secretAccessKey,
        sessionToken: input.app.config.sessionToken,
      },
      endpoint: input.app.config.endpoint,
    });

    const subscriptionArn = input.block.lifecycle?.signals?.subscriptionArn;

    // We must ensure that subscription exists.
    if (subscriptionArn) {
      const subscriptionExists = await checkTopicSubscriptionExists(
        client,
        input.block.config.topicArn,
        subscriptionArn,
      );
      if (subscriptionExists) {
        await kv.block.delete([subscriptionConfirmationKey]);

        return {
          newStatus: "ready",
        };
      }

      const rawSubscriptionState = await kv.block.get(
        subscriptionConfirmationKey,
      );

      if (rawSubscriptionState.value) {
        const subscriptionState =
          rawSubscriptionState.value as SubscriptionState;

        switch (subscriptionState.status) {
          case SubscriptionStatus.PENDING:
            if (!subscriptionState.createdAt) {
              // Should never happen with pending status.
              console.warn(
                "Pending subscription doesn't have initialization time",
              );

              break;
            }

            // In case we weren't able to receive a confirmation
            // message within a reasonable amount of time, we
            // retry creating a subscription.
            if (
              (Date.now() - subscriptionState.createdAt) / 1000 >
              subscriptionTimeoutSeconds
            ) {
              break;
            }

            return {
              newStatus: "in_progress",
              nextScheduleDelay: subscriptionRecheckSeconds,
            };
          case SubscriptionStatus.FAILED:
            return {
              signalUpdates: {
                subscriptionArn: null,
              },
              newStatus: "failed",
              customStatusDescription: subscriptionState.description,
            };
        }
      }
    }

    const endpointURL = input.block.http!.url;

    const command = new SubscribeCommand({
      TopicArn: input.block.config.topicArn,
      Protocol: endpointURL.startsWith("https") ? "https" : "http",
      Endpoint: endpointURL,
      Attributes: input.block.config.attributes,
      ReturnSubscriptionArn: true,
    } as SubscribeCommandInput);

    const response = await client.send(command);

    if (response.$metadata.httpStatusCode !== 200) {
      const errMsg = `Couldn't issue SNS Subscribe command, statusCode: ${response.$metadata.httpStatusCode}`;

      console.error(errMsg);

      return {
        signalUpdates: {
          subscriptionArn: null,
        },
        newStatus: "failed",
        customStatusDescription: errMsg,
      };
    }

    await kv.block.set({
      key: subscriptionConfirmationKey,
      ttl: subscriptionTimeoutSeconds,
      value: {
        status: SubscriptionStatus.PENDING,
        createdAt: Date.now(),
      } as SubscriptionState,
    });

    return {
      signalUpdates: {
        subscriptionArn: response.SubscriptionArn,
      },
      newStatus: "in_progress",
      nextScheduleDelay: subscriptionRecheckSeconds,
    };
  },
  async onDrain(input: EntityInput) {
    const subscriptionArn = input.block.lifecycle?.signals?.subscriptionArn;

    if (subscriptionArn) {
      await kv.block.delete([subscriptionConfirmationKey]);

      try {
        await deleteTopicSubscription(
          input.app,
          input.block.config.region,
          subscriptionArn,
        );
      } catch (err: any) {
        console.error(err.message);

        return {
          newStatus: "draining_failed",
          customStatusDescription: err.message,
        };
      }
    }

    return {
      newStatus: "drained",
    };
  },
  http: {
    async onRequest(input: EntityOnHTTPRequestInput) {
      await new Promise<void>((resolve, reject) => {
        validator.validate(input.request.body, async (err) => {
          if (err) {
            console.error(`SNS message verification failed: ${err.message}`);
            return reject(err);
          }

          try {
            await handleTopicSubscriptionMesage(input.request.body);
            resolve();
          } catch (e) {
            reject(e);
          }
        });
      });

      await http.respond(input.request.requestId, {
        statusCode: 200,
      });
    },
  },
};

async function handleTopicSubscriptionMesage(input: any) {
  switch (input.Type) {
    case "SubscriptionConfirmation":
      try {
        const response = await fetch(input.SubscribeURL);

        if (response.status !== 200) {
          const errMsg = `Confirming subscription, status code: ${response.status}`;

          console.error(errMsg);

          kv.block.set({
            key: subscriptionConfirmationKey,
            value: {
              status: SubscriptionStatus.FAILED,
              description: errMsg,
            } as SubscriptionState,
          });
        }
      } catch (error: any) {
        const errMsg = `Sending confirm subscription requeste: ${error.message}`;

        console.error(errMsg);

        kv.block.set({
          key: subscriptionConfirmationKey,
          value: {
            status: SubscriptionStatus.FAILED,
            description: errMsg,
          } as SubscriptionState,
        });
      }

      lifecycle.sync();

      break;
    case "Notification":
      await events.emit({
        payload: {
          message: input.Message,
          messageId: input.MessageId,
          timestamp: input.Timestamp,
        },
      });

      break;
    default:
      console.warn(`Unexpected SNS message type: ${input.Type}`);
  }
}

async function checkTopicSubscriptionExists(
  client: SNSClient,
  topicArn: string,
  subscriptionArn: string,
  nextToken?: string,
): Promise<boolean> {
  const command = new ListSubscriptionsByTopicCommand({
    TopicArn: topicArn,
    NextToken: nextToken,
  } as ListSubscriptionsByTopicCommandInput);

  const response = await client.send(command);

  // List subscriptions doesn't return Arns for unconfirmed subscriptions.
  if (
    response.Subscriptions?.findIndex((v) => {
      return v.SubscriptionArn === subscriptionArn;
    }) !== -1
  ) {
    return true;
  }

  if (response.NextToken) {
    return checkTopicSubscriptionExists(
      client,
      topicArn,
      subscriptionArn,
      response.NextToken,
    );
  }

  return false;
}

async function deleteTopicSubscription(
  app: AppContext,
  blockRegion: string,
  subscriptionArn: string,
) {
  const client = new SNSClient({
    region: blockRegion,
    credentials: {
      accessKeyId: app.config.accessKeyId,
      secretAccessKey: app.config.secretAccessKey,
      sessionToken: app.config.sessionToken,
    },
    endpoint: app.config.endpoint,
  });

  const command = new UnsubscribeCommand({
    SubscriptionArn: subscriptionArn,
  } as UnsubscribeCommandInput);

  const response = await client.send(command);

  if (response.$metadata.httpStatusCode !== 200) {
    throw new Error(
      `Couldn't issue SNS unsubscribe command, statusCode: ${response.$metadata.httpStatusCode}`,
    );
  }

  await kv.block.delete([subscriptionConfirmationKey]);
}
