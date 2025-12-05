# AWS ToolKit App

A Flows app that provides blocks for integrating with AWS services. Currently supports AWS Simple Notification Service (SNS).

## Features

- Secure credential management with support for IAM and STS temporary credentials
- Type-safe configuration based on AWS API specifications
- Custom endpoint support for testing with LocalStack or other AWS-compatible services

## Available Blocks

### Subscribe to SNS Topic

Subscribes to an AWS SNS topic and emits messages as events when notifications are received.

**Configuration:**

- **Region** (required): AWS region where the SNS topic is located (e.g., `us-east-1`)
- **Topic ARN** (required): The Amazon Resource Name of the topic (e.g., `arn:aws:sns:us-east-1:123456789012:MyTopic`)
- **Attributes** (optional): Map of subscription attributes (e.g., `{"FilterPolicy": "..."}`)

**Output:**

- **On Message**: Emitted when a notification is received
  - `message`: The message text content
  - `messageId`: Unique identifier for the message
  - `timestamp`: When the message was published

**How it works:**

1. Creates an HTTP(S) subscription to your SNS topic
2. Automatically confirms the subscription via SNS confirmation URL
3. Validates incoming messages using SNS message signature verification
4. Emits received notifications as events
5. Automatically unsubscribes when the block is removed

## Quick Start

1. **Configure AWS Credentials**:
   - `accessKeyId`: Your AWS access key
   - `secretAccessKey`: Your AWS secret key
   - `sessionToken`: Optional session token for temporary credentials

2. **Use Specific Resource Blocks**:
   - Choose from pre-built blocks like "Subscribe to SNS topic"
   - Each block provides typed configuration fields based on AWS API schemas
