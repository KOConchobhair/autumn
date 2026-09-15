import { SQSClient } from "@aws-sdk/client-sqs";
import {
	DEFAULT_AWS_REGION,
	extractRegionFromQueueUrl,
} from "@/external/aws/awsRegionUtils.js";
import { getMiscMainRedis } from "@/external/redis/miscCache/miscRedisInstances.js";
import { RedisDedupStore } from "./backends/rabbitmq/dedupStore.js";
import { RabbitMqQueueClient } from "./backends/rabbitmq/RabbitMqQueueClient.js";
import { isRabbitMqQueueUrl } from "./backends/rabbitmq/topology.js";

// ============ Backend selection ============
/** `QUEUE_BACKEND=rabbitmq`, or any `rabbitmq://` queue URL, swaps the AWS
 *  client for an SQS-command-compatible RabbitMQ client. Call sites keep the
 *  `SQSClient` type; only the seven commands Autumn uses are implemented. */
const useRabbitMqBackend = ({ queueUrl }: { queueUrl?: string }): boolean =>
	process.env.QUEUE_BACKEND === "rabbitmq" || isRabbitMqQueueUrl(queueUrl);

const createQueueClient = ({ queueUrl }: { queueUrl?: string }): SQSClient => {
	const resolvedQueueUrl = queueUrl ?? process.env.SQS_QUEUE_URL_V2;
	if (useRabbitMqBackend({ queueUrl: resolvedQueueUrl }) && resolvedQueueUrl) {
		return new RabbitMqQueueClient({
			queueUrl: resolvedQueueUrl,
			dedupStore: new RedisDedupStore(() => getMiscMainRedis()),
		}) as unknown as SQSClient;
	}
	return new SQSClient(getSqsClientConfig({ queueUrl: resolvedQueueUrl }));
};

// ============ FIFO Queue (primary) ============

/** Returns a base endpoint URL if the queue URL points to a non-AWS host (e.g. ElasticMQ). */
export const extractLocalEndpoint = ({
	queueUrl,
}: {
	queueUrl: string | undefined;
}): string | undefined => {
	if (!queueUrl) return undefined;
	try {
		const url = new URL(queueUrl);
		if (url.hostname.endsWith("amazonaws.com")) return undefined;
		return `${url.protocol}//${url.host}`;
	} catch {
		return undefined;
	}
};

const getSqsClientConfig = ({ queueUrl }: { queueUrl?: string } = {}) => {
	const resolvedQueueUrl = queueUrl ?? process.env.SQS_QUEUE_URL_V2;
	const endpoint = extractLocalEndpoint({ queueUrl: resolvedQueueUrl });
	const region =
		extractRegionFromQueueUrl({ queueUrl: resolvedQueueUrl }) ||
		DEFAULT_AWS_REGION;

	return {
		region,
		...(endpoint ? { endpoint } : {}),
		credentials: {
			accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
		},
	};
};

const getSqsClientCacheKey = ({ queueUrl }: { queueUrl?: string } = {}) => {
	const resolvedQueueUrl = queueUrl ?? process.env.SQS_QUEUE_URL_V2;
	const endpoint = extractLocalEndpoint({ queueUrl: resolvedQueueUrl });
	const region =
		extractRegionFromQueueUrl({ queueUrl: resolvedQueueUrl }) ||
		DEFAULT_AWS_REGION;

	return `${region}:${endpoint ?? "aws"}:${resolvedQueueUrl ?? "default"}`;
};

const sqsClientsByCacheKey = new Map<string, SQSClient>();

let sqsClient = createQueueClient({});
sqsClientsByCacheKey.set(getSqsClientCacheKey(), sqsClient);

export const sqs = sqsClient;

/** Recreates the SQS client with fresh connections */
export const recreateSqsClient = ({
	queueUrl,
}: {
	queueUrl?: string;
} = {}): SQSClient => {
	console.log(`[SQS] Recreating SQS client (stale connection suspected)`);
	const cacheKey = getSqsClientCacheKey({ queueUrl });
	const existingClient = sqsClientsByCacheKey.get(cacheKey);
	existingClient?.destroy();

	const nextClient = createQueueClient({ queueUrl });
	sqsClientsByCacheKey.set(cacheKey, nextClient);

	if (!queueUrl || queueUrl === process.env.SQS_QUEUE_URL_V2) {
		sqsClient = nextClient;
	}

	return nextClient;
};

/** Get the current SQS client (use this instead of direct sqs export for refreshable access) */
export const getSqsClient = ({
	queueUrl,
}: {
	queueUrl?: string;
} = {}): SQSClient => {
	const cacheKey = getSqsClientCacheKey({ queueUrl });
	const existingClient = sqsClientsByCacheKey.get(cacheKey);
	if (existingClient) return existingClient;

	const nextClient = createQueueClient({ queueUrl });
	sqsClientsByCacheKey.set(cacheKey, nextClient);
	return nextClient;
};

export const QUEUE_URL = process.env.SQS_QUEUE_URL_V2 || "";
