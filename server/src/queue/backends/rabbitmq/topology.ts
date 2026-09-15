/**
 * RabbitMQ topology for the SQS-compatible queue backend.
 *
 * Queue URLs use the scheme `rabbitmq://<queue-name>`; a name ending in
 * `.fifo` keeps SQS's FIFO semantics (per-MessageGroupId ordering, content
 * dedup) so the upstream `queueUrl.endsWith(".fifo")` checks keep working.
 *
 * Per logical queue `Q`:
 *   exchange  Q                (direct)   — immediate delivery
 *   queue     Q                (quorum)   — standard queue, bound with key Q
 *   queue     Q.p0 .. Q.p{N-1} (quorum, single-active-consumer) — FIFO partitions,
 *             bound with key p{i}; MessageGroupId → partition by hash
 *   queue     Q.dlq            (quorum)   — dead letters after x-delivery-limit
 *   exchange  Q.delay.{s}      (fanout)   — one per delay bucket
 *   queue     Q.delay.{s}      (quorum, x-message-ttl = s*1000, DLX → Q)
 *
 * Delayed messages are published to the bucket exchange with the *final*
 * routing key; on TTL expiry RabbitMQ dead-letters them into exchange Q with
 * the original routing key, which lands them on the right partition.
 */

export const RABBITMQ_URL_SCHEME = "rabbitmq:";

/** SQS caps DelaySeconds at 900. Buckets are queue-level TTLs, so a message
 *  waits for the smallest bucket >= its requested delay. */
export const DELAY_BUCKETS_SECONDS = [1, 5, 15, 30, 60, 120, 300, 600, 900] as const;

export const DEFAULT_FIFO_PARTITIONS = 32;
/** Total deliveries before dead-lettering (SQS maxReceiveCount semantics). */
export const DEFAULT_DELIVERY_LIMIT = 10;
export const DEFAULT_STANDARD_PREFETCH = 10;
/** SQS FIFO content-based dedup window. */
export const DEDUP_WINDOW_MS = 5 * 60 * 1000;

export interface RabbitMqBackendConfig {
	amqpUrl: string;
	fifoPartitions: number;
	deliveryLimit: number;
	standardPrefetch: number;
	/** How often a FIFO consumer checks whether it owns more than its share. */
	rebalanceIntervalMs: number;
	/** How long a released partition stays unsubscribed before re-offering. */
	releaseCooldownMs: number;
}

export const DEFAULT_REBALANCE_INTERVAL_MS = 15_000;
export const DEFAULT_RELEASE_COOLDOWN_MS = 60_000;

export const readRabbitMqBackendConfig = (
	env: NodeJS.ProcessEnv = process.env,
): RabbitMqBackendConfig => {
	const amqpUrl = env.RABBITMQ_URL;
	if (!amqpUrl) {
		throw new Error(
			"[RabbitMQ] QUEUE_BACKEND=rabbitmq requires RABBITMQ_URL (amqp(s)://user:pass@host:5672/vhost)",
		);
	}
	const int = (value: string | undefined, fallback: number) => {
		const parsed = Number.parseInt(value ?? "", 10);
		return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
	};
	return {
		amqpUrl,
		fifoPartitions: int(env.RABBITMQ_FIFO_PARTITIONS, DEFAULT_FIFO_PARTITIONS),
		deliveryLimit: int(env.RABBITMQ_DELIVERY_LIMIT, DEFAULT_DELIVERY_LIMIT),
		standardPrefetch: int(
			env.RABBITMQ_STANDARD_PREFETCH,
			DEFAULT_STANDARD_PREFETCH,
		),
		rebalanceIntervalMs: int(
			env.RABBITMQ_REBALANCE_INTERVAL_MS,
			DEFAULT_REBALANCE_INTERVAL_MS,
		),
		releaseCooldownMs: int(
			env.RABBITMQ_RELEASE_COOLDOWN_MS,
			DEFAULT_RELEASE_COOLDOWN_MS,
		),
	};
};

export const isRabbitMqQueueUrl = (queueUrl: string | undefined): boolean => {
	if (!queueUrl) return false;
	try {
		return new URL(queueUrl).protocol === RABBITMQ_URL_SCHEME;
	} catch {
		return false;
	}
};

export interface ParsedQueueUrl {
	/** Logical queue name, e.g. `autumn-primary.fifo`. */
	name: string;
	isFifo: boolean;
}

export const parseRabbitMqQueueUrl = (queueUrl: string): ParsedQueueUrl => {
	const url = new URL(queueUrl);
	if (url.protocol !== RABBITMQ_URL_SCHEME) {
		throw new Error(`[RabbitMQ] Not a rabbitmq:// queue URL: ${queueUrl}`);
	}
	// rabbitmq://autumn-primary.fifo  → host carries the name (URL lowercases it,
	// which is fine — queue names are ours). Also accept rabbitmq:///name.
	const name = (url.hostname || url.pathname.replace(/^\/+/, "")).trim();
	if (!name) throw new Error(`[RabbitMQ] Empty queue name in ${queueUrl}`);
	return { name, isFifo: name.endsWith(".fifo") };
};

// FNV-1a 32-bit: stable across processes (Bun.hash is not guaranteed stable
// across versions and we need every publisher to agree on the partition).
export const fnv1a32 = (input: string): number => {
	let hash = 0x811c9dc5;
	for (let i = 0; i < input.length; i++) {
		hash ^= input.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

export const partitionForGroup = ({
	messageGroupId,
	partitions,
}: {
	messageGroupId: string;
	partitions: number;
}): number => fnv1a32(messageGroupId) % partitions;

export const partitionQueueName = (name: string, partition: number) =>
	`${name}.p${partition}`;
export const dlqName = (name: string) => `${name}.dlq`;
export const delayName = (name: string, seconds: number) =>
	`${name}.delay.${seconds}`;

export const delayBucketFor = (delaySeconds: number): number => {
	for (const bucket of DELAY_BUCKETS_SECONDS) {
		if (delaySeconds <= bucket) return bucket;
	}
	return DELAY_BUCKETS_SECONDS[DELAY_BUCKETS_SECONDS.length - 1];
};

/** Routing key inside exchange `name` for a message. */
export const routingKeyFor = ({
	parsed,
	messageGroupId,
	partitions,
}: {
	parsed: ParsedQueueUrl;
	messageGroupId?: string;
	partitions: number;
}): string => {
	if (!parsed.isFifo) return parsed.name;
	const group = messageGroupId ?? "";
	return `p${partitionForGroup({ messageGroupId: group, partitions })}`;
};

export const workQueueNames = ({
	parsed,
	partitions,
}: {
	parsed: ParsedQueueUrl;
	partitions: number;
}): string[] =>
	parsed.isFifo
		? Array.from({ length: partitions }, (_, i) =>
				partitionQueueName(parsed.name, i),
			)
		: [parsed.name];

const quorumArgs = ({
	name,
	deliveryLimit,
	singleActiveConsumer,
}: {
	name: string;
	deliveryLimit: number;
	singleActiveConsumer: boolean;
}) => ({
	"x-queue-type": "quorum" as const,
	// RabbitMQ counts *re*deliveries; RABBITMQ_DELIVERY_LIMIT is defined like
	// SQS maxReceiveCount (total deliveries), so subtract one.
	"x-delivery-limit": Math.max(1, deliveryLimit - 1),
	"x-dead-letter-exchange": "",
	"x-dead-letter-routing-key": dlqName(name),
	...(singleActiveConsumer ? { "x-single-active-consumer": true } : {}),
});

/** Declarations passed to rabbitmq-client so they are re-asserted on every
 *  reconnect. All declarations are idempotent. */
export const buildTopology = ({
	parsed,
	config,
}: {
	parsed: ParsedQueueUrl;
	config: RabbitMqBackendConfig;
}) => {
	const { name } = parsed;
	const work = workQueueNames({ parsed, partitions: config.fifoPartitions });

	const exchanges = [
		{ exchange: name, type: "direct" as const, durable: true },
		...DELAY_BUCKETS_SECONDS.map((s) => ({
			exchange: delayName(name, s),
			type: "fanout" as const,
			durable: true,
		})),
	];

	const queues = [
		...work.map((queue) => ({
			queue,
			durable: true,
			arguments: quorumArgs({
				name,
				deliveryLimit: config.deliveryLimit,
				singleActiveConsumer: parsed.isFifo,
			}),
		})),
		{
			queue: dlqName(name),
			durable: true,
			arguments: { "x-queue-type": "quorum" as const },
		},
		...DELAY_BUCKETS_SECONDS.map((s) => ({
			queue: delayName(name, s),
			durable: true,
			arguments: {
				"x-queue-type": "quorum" as const,
				"x-message-ttl": s * 1000,
				"x-dead-letter-exchange": name,
			},
		})),
	];

	const queueBindings = [
		...work.map((queue, i) => ({
			exchange: name,
			queue,
			routingKey: parsed.isFifo ? `p${i}` : name,
		})),
		...DELAY_BUCKETS_SECONDS.map((s) => ({
			exchange: delayName(name, s),
			queue: delayName(name, s),
			routingKey: "",
		})),
	];

	return { exchanges, queues, queueBindings, workQueues: work };
};
