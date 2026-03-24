import {
  AppBlock,
  EntityInput,
  EntityOnHTTPRequestInput,
  events,
  kv,
  http,
} from "@slflows/sdk/v1";
import {
  EventBridgeClient,
  CreateConnectionCommand,
  CreateApiDestinationCommand,
  PutRuleCommand,
  PutTargetsCommand,
  RemoveTargetsCommand,
  DeleteRuleCommand,
  DeleteApiDestinationCommand,
  DeleteConnectionCommand,
  DescribeRuleCommand,
  DescribeConnectionCommand,
} from "@aws-sdk/client-eventbridge";
import { randomBytes } from "crypto";

const kvKeyApiKey = "api-key";
const kvKeyConnectionArn = "connection-arn";
const kvKeyApiDestinationArn = "api-destination-arn";
const kvKeyRuleArn = "rule-arn";

function sanitizeResourceName(blockId: string, suffix: string): string {
  const sanitized = blockId.replace(/[^a-zA-Z0-9_-]/g, "-");
  const prefix = `flows-${sanitized}`;
  const name = `${prefix}-${suffix}`;
  return name.slice(0, 64);
}

function createClient(input: EntityInput): EventBridgeClient {
  return new EventBridgeClient({
    region: input.block.config.region,
    credentials: {
      accessKeyId: input.app.config.accessKeyId,
      secretAccessKey: input.app.config.secretAccessKey,
      sessionToken: input.app.config.sessionToken,
    },
    endpoint: input.app.config.endpoint,
  });
}

async function ensureApiKey(): Promise<string> {
  let apiKey = (await kv.block.get(kvKeyApiKey)).value as string | null;
  if (!apiKey) {
    apiKey = randomBytes(32).toString("hex");
    await kv.block.set({ key: kvKeyApiKey, value: apiKey });
  }
  return apiKey;
}

async function ensureConnection(
  client: EventBridgeClient,
  name: string,
  apiKey: string,
): Promise<string> {
  const existing = (await kv.block.get(kvKeyConnectionArn)).value as
    | string
    | null;
  if (existing) return existing;

  let arn: string;
  try {
    const resp = await client.send(
      new CreateConnectionCommand({
        Name: name,
        AuthorizationType: "API_KEY",
        AuthParameters: {
          ApiKeyAuthParameters: {
            ApiKeyName: "x-api-key",
            ApiKeyValue: apiKey,
          },
        },
      }),
    );
    arn = resp.ConnectionArn!;
  } catch (err: any) {
    if (err.name === "ResourceAlreadyExistsException") {
      const desc = await client.send(
        new DescribeConnectionCommand({ Name: name }),
      );
      arn = desc.ConnectionArn!;
    } else {
      throw err;
    }
  }

  await kv.block.set({ key: kvKeyConnectionArn, value: arn });
  return arn;
}

async function ensureApiDestination(
  client: EventBridgeClient,
  name: string,
  connectionArn: string,
  endpoint: string,
): Promise<string> {
  const existing = (await kv.block.get(kvKeyApiDestinationArn)).value as
    | string
    | null;
  if (existing) return existing;

  let arn: string;
  try {
    const resp = await client.send(
      new CreateApiDestinationCommand({
        Name: name,
        ConnectionArn: connectionArn,
        InvocationEndpoint: endpoint,
        HttpMethod: "POST",
        InvocationRateLimitPerSecond: 300,
      }),
    );
    arn = resp.ApiDestinationArn!;
  } catch (err: any) {
    if (err.name === "ResourceAlreadyExistsException") {
      // Delete and recreate to ensure correct endpoint
      try {
        await client.send(new DeleteApiDestinationCommand({ Name: name }));
      } catch {
        // ignore
      }
      const resp = await client.send(
        new CreateApiDestinationCommand({
          Name: name,
          ConnectionArn: connectionArn,
          InvocationEndpoint: endpoint,
          HttpMethod: "POST",
          InvocationRateLimitPerSecond: 300,
        }),
      );
      arn = resp.ApiDestinationArn!;
    } else {
      throw err;
    }
  }

  await kv.block.set({ key: kvKeyApiDestinationArn, value: arn });
  return arn;
}

async function ensureRule(
  client: EventBridgeClient,
  name: string,
  eventBusName: string,
  eventPattern: string,
): Promise<string> {
  const existing = (await kv.block.get(kvKeyRuleArn)).value as string | null;
  if (existing) return existing;

  const resp = await client.send(
    new PutRuleCommand({
      Name: name,
      EventBusName: eventBusName,
      EventPattern: eventPattern,
      State: "ENABLED",
    }),
  );
  const arn = resp.RuleArn!;

  await kv.block.set({ key: kvKeyRuleArn, value: arn });
  return arn;
}

export const subscribeEventBridgeRule: AppBlock = {
  name: "Subscribe to EventBridge Rule",
  description:
    "Creates an EventBridge rule with an API Destination target, forwarding matched events to this block's HTTP endpoint.",
  config: {
    region: {
      name: "Region",
      description: "AWS region where EventBridge is located.",
      type: "string",
      required: true,
      fixed: true,
    },
    eventBusName: {
      name: "Event Bus Name",
      description: "The name or ARN of the event bus to monitor.",
      type: "string",
      required: false,
      default: "default",
      fixed: true,
    },
    eventPattern: {
      name: "Event Pattern",
      description:
        'JSON event pattern for matching events (e.g. {"source": ["aws.ec2"]}).',
      type: "string",
      required: true,
      fixed: true,
    },
    roleArn: {
      name: "Role ARN",
      description:
        "IAM role ARN that EventBridge assumes to invoke the API Destination. The role must trust events.amazonaws.com (sts:AssumeRole) and allow events:InvokeApiDestination.",
      type: "string",
      required: true,
      fixed: true,
    },
  },
  outputs: {
    default: {
      name: "On Event",
      description: "Emitted EventBridge event payload",
      type: {
        type: "object",
        properties: {
          payload: {
            type: "object",
            description: "Incoming EventBridge event payload.",
            properties: {
              event: {
                type: "any",
                description: "Full EventBridge event object.",
              },
              source: {
                type: "string",
                description: 'Event source (e.g. "aws.ec2").',
              },
              detailType: {
                type: "string",
                description: "Detail type of the event.",
              },
              detail: {
                type: "any",
                description: "Event detail payload.",
              },
              time: {
                type: "string",
                description: "Event timestamp.",
              },
            },
            required: ["event", "source", "detailType", "detail", "time"],
          },
        },
        required: ["payload"],
      },
    },
  },
  signals: {
    ruleArn: {
      name: "Rule ARN",
      description: "The ARN of the EventBridge rule.",
    },
    connectionArn: {
      name: "Connection ARN",
      description: "The ARN of the EventBridge connection.",
    },
    apiDestinationArn: {
      name: "API Destination ARN",
      description: "The ARN of the EventBridge API Destination.",
    },
  },
  async onSync(input: EntityInput) {
    const signals = input.block.lifecycle?.signals;
    const client = createClient(input);

    // If all signals exist, verify resources still exist
    if (
      signals?.ruleArn &&
      signals?.connectionArn &&
      signals?.apiDestinationArn
    ) {
      try {
        const connName = sanitizeResourceName(input.block.id, "conn");
        const ruleName = sanitizeResourceName(input.block.id, "rule");

        await client.send(
          new DescribeRuleCommand({
            Name: ruleName,
            EventBusName: input.block.config.eventBusName,
          }),
        );
        await client.send(new DescribeConnectionCommand({ Name: connName }));

        return { newStatus: "ready" };
      } catch (err: any) {
        if (
          err.name === "ResourceNotFoundException" ||
          err.name === "ResourceNotFoundFault"
        ) {
          console.warn("Resources missing, recreating from scratch");
          await kv.block.delete([
            kvKeyConnectionArn,
            kvKeyApiDestinationArn,
            kvKeyRuleArn,
          ]);
        } else {
          throw err;
        }
      }
    }

    const blockId = input.block.id;
    const connName = sanitizeResourceName(blockId, "conn");
    const destName = sanitizeResourceName(blockId, "dest");
    const ruleName = sanitizeResourceName(blockId, "rule");

    try {
      const apiKey = await ensureApiKey();
      const connArn = await ensureConnection(client, connName, apiKey);
      const destArn = await ensureApiDestination(
        client,
        destName,
        connArn,
        input.block.http!.url,
      );
      const ruleArn = await ensureRule(
        client,
        ruleName,
        input.block.config.eventBusName,
        input.block.config.eventPattern,
      );

      await client.send(
        new PutTargetsCommand({
          Rule: ruleName,
          EventBusName: input.block.config.eventBusName,
          Targets: [
            {
              Id: "flows-target",
              Arn: destArn,
              RoleArn: input.block.config.roleArn,
              HttpParameters: {
                HeaderParameters: {
                  "x-api-key": apiKey,
                },
              },
            },
          ],
        }),
      );

      return {
        signalUpdates: {
          ruleArn,
          connectionArn: connArn,
          apiDestinationArn: destArn,
        },
        newStatus: "ready",
      };
    } catch (err: any) {
      return {
        newStatus: "failed",
        customStatusDescription: err.message,
      };
    }
  },
  async onDrain(input: EntityInput) {
    const client = createClient(input);
    const blockId = input.block.id;
    const connName = sanitizeResourceName(blockId, "conn");
    const destName = sanitizeResourceName(blockId, "dest");
    const ruleName = sanitizeResourceName(blockId, "rule");
    const eventBusName = input.block.config.eventBusName;
    const errors: string[] = [];

    // 1. Remove targets
    try {
      await client.send(
        new RemoveTargetsCommand({
          Rule: ruleName,
          EventBusName: eventBusName,
          Ids: ["flows-target"],
        }),
      );
    } catch (err: any) {
      if (err.name !== "ResourceNotFoundException") {
        console.error(`Failed to remove targets: ${err.message}`);
        errors.push(err.message);
      }
    }

    // 2. Delete rule
    try {
      await client.send(
        new DeleteRuleCommand({
          Name: ruleName,
          EventBusName: eventBusName,
        }),
      );
    } catch (err: any) {
      if (err.name !== "ResourceNotFoundException") {
        console.error(`Failed to delete rule: ${err.message}`);
        errors.push(err.message);
      }
    }

    // 3. Delete API destination
    try {
      await client.send(new DeleteApiDestinationCommand({ Name: destName }));
    } catch (err: any) {
      if (err.name !== "ResourceNotFoundException") {
        console.error(`Failed to delete API destination: ${err.message}`);
        errors.push(err.message);
      }
    }

    // 4. Delete connection
    try {
      await client.send(new DeleteConnectionCommand({ Name: connName }));
    } catch (err: any) {
      if (err.name !== "ResourceNotFoundException") {
        console.error(`Failed to delete connection: ${err.message}`);
        errors.push(err.message);
      }
    }

    // 5. Clean up KV
    await kv.block.delete([
      kvKeyApiKey,
      kvKeyConnectionArn,
      kvKeyApiDestinationArn,
      kvKeyRuleArn,
    ]);

    if (errors.length > 0) {
      return {
        newStatus: "draining_failed",
        customStatusDescription: errors.join("; "),
      };
    }

    return {
      newStatus: "drained",
    };
  },
  http: {
    async onRequest(input: EntityOnHTTPRequestInput) {
      const storedApiKey = (await kv.block.get(kvKeyApiKey)).value as
        | string
        | null;

      const requestApiKey = input.request.headers?.["X-Api-Key"];

      if (!storedApiKey || requestApiKey !== storedApiKey) {
        await http.respond(input.request.requestId, {
          statusCode: 401,
          body: "Unauthorized",
        });
        return;
      }

      const body = input.request.body;

      await events.emit({
        payload: {
          event: body,
          source: body.source ?? "",
          detailType: body["detail-type"] ?? "",
          detail: body.detail ?? {},
          time: body.time ?? "",
        },
      });

      await http.respond(input.request.requestId, {
        statusCode: 200,
      });
    },
  },
};
