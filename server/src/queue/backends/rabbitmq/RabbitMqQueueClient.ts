import { randomUUID } from "node:crypto";
import type {
	ChangeMessageVisibilityCommandInput,
	DeleteMessageBatchCommandInput,
	DeleteMessageBatchCommandOutput,
	DeleteMessageCommandInput,
	GetQueueAttributesCommandInput,
	GetQueueAttributesCommandOutput,
	Message,
	ReceiveMessageCommandInput,
	ReceiveMessageCommandOutput,
	SendMessageBatchCommandInput,
	SendMessageBatchCommandOutput,
	SendMessageBatchRequestEntry,
	SendMessageCommandInput,
	SendMessageCommandOutput,
} from "@aws-sdk/client-sqs";
import {
	type AsyncMessage,
	Connection,
	type Consumer,
	ConsumerStatus,
	type Publisher,
} from "rabbitmq-client";
import { type DedupStore, NoopDedupStore } from "./dedupStore.js";
import {
	buildTopology,
	delayBucketFor,
	delayName,
	type ParsedQueueUrl,
	parseRabbitMqQueueUrl,
	type RabbitMqBackendConfig,
	readRabbitMqBackendConfig,
	routingKeyFor,
} from "./topology.js";

/**
 * A drop-in for `SQSClient` that speaks the seven SQS commands Autumn uses:
 * SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage,
 * DeleteMessageBatch, ChangeMessageVisibility, GetQueueAttributes.
 *
 * Semantics preserved from SQS:
 *  - FIFO per-group ordering: groups hash to a partition queue that has a
 *    single active consumer and prefetch 1, so one message per partition is
 *    in flight cluster-wide (SQS: one per group). Stricter, never looser.
 *  - Visibility timeout: a received message that is not deleted within the
 *    timeout is nacked-with-requeue; ChangeMessageVisibility resets the timer.
 *    Quorum queues bump `x-delivery-count` on each redelivery, which becomes
 *    ApproximateReceiveCount, and dead-letter to `<Q>.dlq` past the limit.
 *  - DelaySeconds via TTL bucket queues; content dedup via `DedupStore`.
 *
 * Call sites cast this to `SQSClient` in `initSqs.ts`; nothing else upstream
 * needs to know.
 */

const HEADER_SENT_AT_MS = "x-autumn-sent-at-ms";
const HEADER_GROUP_ID = "x-autumn-group-id";
const HEADER_DELIVERY_COUNT = "x-delivery-count";

type Settle = (status: ConsumerStatus) => void;

interface PartitionState {
	queue: string;
	consumer: Consumer | null;
	/** We have received a delivery since (re)subscribing — under SAC that
	 *  means this process is the active consumer for the partition. */
	active: boolean;
	inFlight: number;
	/** Set while voluntarily released so another process can take over. */
	releasedAt: number | null;
}


interface InFlight {
	receiptHandle: string;
	queue: string;
	msg: AsyncMessage;
	settle: Settle;
	/** Set once ReceiveMessage hands the message to the app. */
	visibilityTimer?: ReturnType<typeof setTimeout>;
	handedOut: boolean;
}

interface SendOptions {
	abortSignal?: AbortSignal;
}

interface CommandLike {
	input?: unknown;
	constructor: { name: string };
}

class AbortError extends Error {
	override name = "AbortError";
}

const sharedConnections = new Map<string, Connection>();

const getSharedConnection = (amqpUrl: string): Connection => {
	let conn = sharedConnections.get(amqpUrl);
	if (!conn) {
		conn = new Connection(amqpUrl);
		conn.on("error", (err) => {
			console.error("[RabbitMQ] connection error:", err?.message ?? err);
		});
		conn.on("connection", () => {
			console.log("[RabbitMQ] connection (re)established");
		});
		sharedConnections.set(amqpUrl, conn);
	}
	return conn;
};

/** Close every shared connection (process shutdown). */
export const closeRabbitMqConnections = async (): Promise<void> => {
	const conns = Array.from(sharedConnections.values());
	sharedConnections.clear();
	await Promise.allSettled(conns.map((c) => c.close()));
};

export class RabbitMqQueueClient {
	private readonly parsed: ParsedQueueUrl;
	private readonly config: RabbitMqBackendConfig;
	private readonly connection: Connection;
	private readonly dedup: DedupStore;
	private readonly topology: ReturnType<typeof buildTopology>;

	private publisher: Publisher | null = null;
	private readonly partitions = new Map<string, PartitionState>();
	private consumersStarted = false;
	private rebalanceTimer: ReturnType<typeof setInterval> | null = null;

	private readonly inFlight = new Map<string, InFlight>();
	/** Receipt handles buffered by consumers, not yet handed to the app. */
	private readonly available: string[] = [];
	private readonly waiters = new Set<() => void>();
	private destroyed = false;

	constructor({
		queueUrl,
		dedupStore,
		config,
		connection,
	}: {
		queueUrl: string;
		dedupStore?: DedupStore;
		config?: RabbitMqBackendConfig;
		connection?: Connection;
	}) {
		this.parsed = parseRabbitMqQueueUrl(queueUrl);
		this.config = config ?? readRabbitMqBackendConfig();
		this.connection = connection ?? getSharedConnection(this.config.amqpUrl);
		this.dedup = dedupStore ?? new NoopDedupStore();
		this.topology = buildTopology({ parsed: this.parsed, config: this.config });
	}

	get queueName(): string {
		return this.parsed.name;
	}

	// ---------------------------------------------------------------- dispatch

	async send(command: CommandLike, options?: SendOptions): Promise<unknown> {
		if (this.destroyed) {
			throw new Error(`[RabbitMQ] client for ${this.parsed.name} is destroyed`);
		}
		const input = command.input as unknown;
		switch (command.constructor.name) {
			case "SendMessageCommand":
				return this.sendMessage(input as SendMessageCommandInput);
			case "SendMessageBatchCommand":
				return this.sendMessageBatch(input as SendMessageBatchCommandInput);
			case "ReceiveMessageCommand":
				return this.receiveMessage(
					input as ReceiveMessageCommandInput,
					options?.abortSignal,
				);
			case "DeleteMessageCommand":
				return this.deleteMessage(input as DeleteMessageCommandInput);
			case "DeleteMessageBatchCommand":
				return this.deleteMessageBatch(input as DeleteMessageBatchCommandInput);
			case "ChangeMessageVisibilityCommand":
				return this.changeMessageVisibility(
					input as ChangeMessageVisibilityCommandInput,
				);
			case "GetQueueAttributesCommand":
				return this.getQueueAttributes(input as GetQueueAttributesCommandInput);
			default:
				throw new Error(
					`[RabbitMQ] unsupported SQS command: ${command.constructor.name}`,
				);
		}
	}

	/** SQSClient parity. Requeues anything in flight and closes channels. */
	destroy(): void {
		this.destroyed = true;
		for (const entry of this.inFlight.values()) {
			if (entry.visibilityTimer) clearTimeout(entry.visibilityTimer);
			entry.settle(ConsumerStatus.REQUEUE);
		}
		this.inFlight.clear();
		this.available.length = 0;
		this.wakeWaiters();
		if (this.rebalanceTimer) clearInterval(this.rebalanceTimer);
		this.rebalanceTimer = null;
		const consumers = Array.from(this.partitions.values())
			.map((p) => p.consumer)
			.filter((c): c is Consumer => Boolean(c));
		this.partitions.clear();
		void Promise.allSettled(consumers.map((c) => c.close()));
		const publisher = this.publisher;
		this.publisher = null;
		void publisher?.close();
	}

	// ----------------------------------------------------------------- publish

	private getPublisher(): Publisher {
		if (!this.publisher) {
			this.publisher = this.connection.createPublisher({
				confirm: true,
				maxAttempts: 3,
				exchanges: this.topology.exchanges,
				queues: this.topology.queues,
				queueBindings: this.topology.queueBindings,
			});
			this.publisher.on("retry", (err) => {
				console.warn(
					`[RabbitMQ] publish retry on ${this.parsed.name}: ${err?.message ?? err}`,
				);
			});
		}
		return this.publisher;
	}

	private async publishOne({
		body,
		messageGroupId,
		messageDeduplicationId,
		delaySeconds,
	}: {
		body: string;
		messageGroupId?: string;
		messageDeduplicationId?: string;
		delaySeconds?: number;
	}): Promise<{ messageId: string; deduplicated: boolean }> {
		const messageId = randomUUID();

		if (this.parsed.isFifo) {
			if (!messageGroupId) {
				throw senderFault(
					"MissingParameter",
					"The request must contain the parameter MessageGroupId.",
				);
			}
			if (messageDeduplicationId) {
				const fresh = await this.dedup.claim({
					queueName: this.parsed.name,
					dedupId: messageDeduplicationId,
				});
				// SQS returns success for a deduplicated message; it just isn't enqueued.
				if (!fresh) return { messageId, deduplicated: true };
			}
		}

		const routingKey = routingKeyFor({
			parsed: this.parsed,
			messageGroupId,
			partitions: this.config.fifoPartitions,
		});
		const exchange =
			delaySeconds && delaySeconds > 0
				? delayName(this.parsed.name, delayBucketFor(delaySeconds))
				: this.parsed.name;

		const sentAtMs = Date.now();
		await this.getPublisher().send(
			{
				exchange,
				routingKey,
				durable: true,
				messageId,
				timestamp: Math.floor(sentAtMs / 1000),
				contentType: "text/plain",
				headers: {
					[HEADER_SENT_AT_MS]: sentAtMs,
					...(messageGroupId ? { [HEADER_GROUP_ID]: messageGroupId } : {}),
				},
			},
			body,
		);
		return { messageId, deduplicated: false };
	}

	private async sendMessage(
		input: SendMessageCommandInput,
	): Promise<SendMessageCommandOutput> {
		if (!input.MessageBody) {
			throw senderFault("MissingParameter", "MessageBody is required.");
		}
		const { messageId } = await this.publishOne({
			body: input.MessageBody,
			messageGroupId: input.MessageGroupId,
			messageDeduplicationId: input.MessageDeduplicationId,
			delaySeconds: input.DelaySeconds,
		});
		return { MessageId: messageId, $metadata: {} };
	}

	private async sendMessageBatch(
		input: SendMessageBatchCommandInput,
	): Promise<SendMessageBatchCommandOutput> {
		const entries: SendMessageBatchRequestEntry[] = input.Entries ?? [];
		const results = await Promise.allSettled(
			entries.map((entry) =>
				this.publishOne({
					body: entry.MessageBody ?? "",
					messageGroupId: entry.MessageGroupId,
					messageDeduplicationId: entry.MessageDeduplicationId,
					delaySeconds: entry.DelaySeconds,
				}),
			),
		);
		const output: SendMessageBatchCommandOutput = {
			Successful: [],
			Failed: [],
			$metadata: {},
		};
		results.forEach((result, i) => {
			const id = entries[i].Id ?? String(i);
			if (result.status === "fulfilled") {
				output.Successful!.push({
					Id: id,
					MessageId: result.value.messageId,
					MD5OfMessageBody: "",
				});
			} else {
				const err = result.reason as Partial<SqsStyleError>;
				output.Failed!.push({
					Id: id,
					Code: err.Code ?? "InternalError",
					Message: err.message ?? String(result.reason),
					SenderFault: Boolean(err.SenderFault),
				});
			}
		});
		return output;
	}

	// ----------------------------------------------------------------- consume

	private ensureConsumers(): void {
		if (this.consumersStarted) return;
		this.consumersStarted = true;
		for (const queue of this.topology.workQueues) {
			this.partitions.set(queue, {
				queue,
				consumer: null,
				active: false,
				inFlight: 0,
				releasedAt: null,
			});
			this.subscribe(queue);
		}
		if (this.parsed.isFifo && this.topology.workQueues.length > 1) {
			this.rebalanceTimer = setInterval(() => {
				void this.rebalance().catch((err) => {
					console.warn(
						`[RabbitMQ] rebalance failed on ${this.parsed.name}: ${err instanceof Error ? err.message : err}`,
					);
				});
			}, this.config.rebalanceIntervalMs);
		}
	}

	private subscribe(queue: string): void {
		const state = this.partitions.get(queue);
		if (!state || state.consumer || this.destroyed) return;
		const prefetch = this.parsed.isFifo ? 1 : this.config.standardPrefetch;
		const consumer = this.connection.createConsumer(
			{
				queue,
				noAck: false,
				qos: { prefetchCount: prefetch },
				concurrency: prefetch,
				// Under single-active-consumer the broker picks the highest priority
				// among consumers present when the slot frees up; random priorities
				// spread partitions on failover. Joining later never preempts, so
				// steady-state spread is handled by rebalance().
				arguments: this.parsed.isFifo
					? { "x-priority": Math.floor(Math.random() * 1_000_000) }
					: undefined,
				// Topology is declared by the publisher too; repeating it here means
				// a consumer-only process (workers) still gets a fully declared queue.
				queueOptions: this.topology.queues.find((q) => q.queue === queue),
				exchanges: this.topology.exchanges,
				queueBindings: this.topology.queueBindings.filter(
					(b) => b.queue === queue,
				),
			},
			(msg) => this.onDelivery(queue, msg),
		);
		consumer.on("error", (err) => {
			console.error(
				`[RabbitMQ] consumer error on ${queue}: ${err?.message ?? err}`,
			);
		});
		state.consumer = consumer;
		state.active = false;
		state.releasedAt = null;
	}

	/** Live process count for this queue, from the broker's consumer count on
	 *  the partitions (every process subscribes to every partition it hasn't
	 *  released, so the max over partitions ≈ number of worker processes). */
	private async countPeers(): Promise<number> {
		const channel = await this.connection.acquire();
		try {
			let peers = 1;
			for (const queue of this.topology.workQueues) {
				const info = await channel.queueDeclare({ queue, passive: true });
				peers = Math.max(peers, info.consumerCount ?? 1);
			}
			return peers;
		} finally {
			await channel.close();
		}
	}

	/** Give up partitions beyond our fair share so idle peers can take them. */
	private async rebalance(): Promise<void> {
		if (this.destroyed) return;

		// Re-offer released partitions after the cooldown.
		const now = Date.now();
		for (const state of this.partitions.values()) {
			if (
				!state.consumer &&
				state.releasedAt !== null &&
				now - state.releasedAt >= this.config.releaseCooldownMs
			) {
				this.subscribe(state.queue);
			}
		}

		const peers = await this.countPeers();
		if (peers <= 1) return;
		const fairShare = Math.ceil(this.topology.workQueues.length / peers);
		const owned = Array.from(this.partitions.values()).filter(
			(p) => p.consumer && p.active,
		);
		const excess = owned.length - fairShare;
		if (excess <= 0) return;

		// Release idle ones first; a partition with a message in flight keeps
		// its consumer until that message settles (no mid-flight requeue).
		const releasable = owned
			.filter((p) => p.inFlight === 0)
			.slice(0, excess);
		for (const state of releasable) {
			const consumer = state.consumer;
			state.consumer = null;
			state.active = false;
			state.releasedAt = Date.now();
			void consumer?.close();
		}
		if (releasable.length > 0) {
			console.log(
				`[RabbitMQ] ${this.parsed.name}: released ${releasable.length} partition(s) (owned ${owned.length}, fair share ${fairShare}, peers ${peers})`,
			);
		}
	}

	/** Holds the AMQP delivery open until the app deletes it (ACK) or the
	 *  visibility timeout fires / client is destroyed (REQUEUE). */
	private onDelivery(queue: string, msg: AsyncMessage): Promise<ConsumerStatus> {
		return new Promise<ConsumerStatus>((resolve) => {
			const state = this.partitions.get(queue);
			if (this.destroyed || !state?.consumer) {
				resolve(ConsumerStatus.REQUEUE);
				return;
			}
			state.active = true;
			state.inFlight++;
			const receiptHandle = randomUUID();
			this.inFlight.set(receiptHandle, {
				receiptHandle,
				queue,
				msg,
				settle: (status) => {
					state.inFlight = Math.max(0, state.inFlight - 1);
					resolve(status);
				},
				handedOut: false,
			});
			this.available.push(receiptHandle);
			this.wakeWaiters();
		});
	}

	private wakeWaiters(): void {
		for (const wake of this.waiters) wake();
		this.waiters.clear();
	}

	private waitForAvailable({
		timeoutMs,
		abortSignal,
	}: {
		timeoutMs: number;
		abortSignal?: AbortSignal;
	}): Promise<void> {
		if (this.available.length > 0 || timeoutMs <= 0) return Promise.resolve();
		if (abortSignal?.aborted) return Promise.reject(new AbortError("aborted"));

		return new Promise<void>((resolve, reject) => {
			let timer: ReturnType<typeof setTimeout>;
			const cleanup = () => {
				clearTimeout(timer);
				this.waiters.delete(wake);
				abortSignal?.removeEventListener("abort", onAbort);
			};
			const wake = () => {
				cleanup();
				resolve();
			};
			const onAbort = () => {
				cleanup();
				reject(new AbortError("aborted"));
			};
			timer = setTimeout(wake, timeoutMs);
			this.waiters.add(wake);
			abortSignal?.addEventListener("abort", onAbort, { once: true });
		});
	}

	private armVisibility(entry: InFlight, seconds: number): void {
		if (entry.visibilityTimer) clearTimeout(entry.visibilityTimer);
		entry.visibilityTimer = setTimeout(() => {
			if (this.inFlight.delete(entry.receiptHandle)) {
				console.warn(
					`[RabbitMQ] visibility timeout (${seconds}s) on ${this.parsed.name}, requeueing ${entry.msg.messageId}`,
				);
				entry.settle(ConsumerStatus.REQUEUE);
			}
		}, seconds * 1000);
	}

	private toSqsMessage(entry: InFlight): Message {
		const headers = (entry.msg.headers ?? {}) as Record<string, unknown>;
		const deliveryCount = Number(headers[HEADER_DELIVERY_COUNT] ?? 0);
		const sentAtMs =
			Number(headers[HEADER_SENT_AT_MS]) ||
			(entry.msg.timestamp ? entry.msg.timestamp * 1000 : Date.now());
		const body =
			typeof entry.msg.body === "string"
				? entry.msg.body
				: Buffer.isBuffer(entry.msg.body)
					? entry.msg.body.toString("utf8")
					: JSON.stringify(entry.msg.body);
		return {
			MessageId: entry.msg.messageId ?? entry.receiptHandle,
			ReceiptHandle: entry.receiptHandle,
			Body: body,
			Attributes: {
				SentTimestamp: String(sentAtMs),
				ApproximateReceiveCount: String(deliveryCount + 1),
				...(headers[HEADER_GROUP_ID]
					? { MessageGroupId: String(headers[HEADER_GROUP_ID]) }
					: {}),
			},
		};
	}

	private async receiveMessage(
		input: ReceiveMessageCommandInput,
		abortSignal?: AbortSignal,
	): Promise<ReceiveMessageCommandOutput> {
		this.ensureConsumers();
		const max = Math.max(1, Math.min(input.MaxNumberOfMessages ?? 1, 10));
		const waitMs = (input.WaitTimeSeconds ?? 0) * 1000;
		const visibility = input.VisibilityTimeout ?? 30;

		await this.waitForAvailable({ timeoutMs: waitMs, abortSignal });

		const messages: Message[] = [];
		while (messages.length < max && this.available.length > 0) {
			const handle = this.available.shift()!;
			const entry = this.inFlight.get(handle);
			if (!entry) continue;
			entry.handedOut = true;
			this.armVisibility(entry, visibility);
			messages.push(this.toSqsMessage(entry));
		}
		return { Messages: messages, $metadata: {} };
	}

	private settleHandle(receiptHandle: string, status: ConsumerStatus): boolean {
		const entry = this.inFlight.get(receiptHandle);
		if (!entry) return false;
		this.inFlight.delete(receiptHandle);
		if (entry.visibilityTimer) clearTimeout(entry.visibilityTimer);
		entry.settle(status);
		return true;
	}

	private async deleteMessage(input: DeleteMessageCommandInput) {
		if (!input.ReceiptHandle || !this.settleHandle(input.ReceiptHandle, ConsumerStatus.ACK)) {
			throw senderFault(
				"ReceiptHandleIsInvalid",
				"The receipt handle is not valid or has expired.",
			);
		}
		return { $metadata: {} };
	}

	private async deleteMessageBatch(
		input: DeleteMessageBatchCommandInput,
	): Promise<DeleteMessageBatchCommandOutput> {
		const output: DeleteMessageBatchCommandOutput = {
			Successful: [],
			Failed: [],
			$metadata: {},
		};
		for (const entry of input.Entries ?? []) {
			const id = entry.Id ?? "";
			if (entry.ReceiptHandle && this.settleHandle(entry.ReceiptHandle, ConsumerStatus.ACK)) {
				output.Successful!.push({ Id: id });
			} else {
				output.Failed!.push({
					Id: id,
					Code: "ReceiptHandleIsInvalid",
					Message: "The receipt handle is not valid or has expired.",
					SenderFault: true,
				});
			}
		}
		return output;
	}

	private async changeMessageVisibility(
		input: ChangeMessageVisibilityCommandInput,
	) {
		const entry = input.ReceiptHandle
			? this.inFlight.get(input.ReceiptHandle)
			: undefined;
		if (!entry) {
			throw senderFault(
				"ReceiptHandleIsInvalid",
				"The receipt handle is not valid or has expired.",
			);
		}
		this.armVisibility(entry, input.VisibilityTimeout ?? 30);
		return { $metadata: {} };
	}

	// -------------------------------------------------------------- attributes

	private async getQueueAttributes(
		_input: GetQueueAttributesCommandInput,
	): Promise<GetQueueAttributesCommandOutput> {
		const channel = await this.connection.acquire();
		try {
			let ready = 0;
			for (const queue of this.topology.workQueues) {
				const info = await channel.queueDeclare({ queue, passive: true });
				ready += info.messageCount ?? 0;
			}
			// Cluster-wide unacked count needs the management HTTP API; the
			// caller (batch-reset backpressure) tolerates an approximation.
			const notVisibleLocal = Array.from(this.inFlight.values()).filter(
				(e) => e.handedOut,
			).length;
			return {
				Attributes: {
					ApproximateNumberOfMessages: String(ready),
					ApproximateNumberOfMessagesNotVisible: String(notVisibleLocal),
					ApproximateNumberOfMessagesDelayed: "0",
				},
				$metadata: {},
			};
		} finally {
			await channel.close();
		}
	}
}

// ------------------------------------------------------------------ errors

interface SqsStyleError extends Error {
	Code: string;
	SenderFault: boolean;
}

const senderFault = (code: string, message: string): SqsStyleError => {
	const err = new Error(message) as SqsStyleError;
	err.name = code;
	err.Code = code;
	err.SenderFault = true;
	return err;
};
