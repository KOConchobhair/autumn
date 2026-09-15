/**
 * Behavioural test for RabbitMqQueueClient against a live broker.
 *   RABBITMQ_URL=amqp://guest:guest@localhost:5672 bun src/queue/backends/rabbitmq/__e2e__/rabbitmq.e2e.ts
 *
 * Uses the real @aws-sdk command classes so dispatch-by-constructor-name is
 * exercised exactly as initWorkers/queueUtils exercise it.
 */
import {
	ChangeMessageVisibilityCommand,
	DeleteMessageBatchCommand,
	DeleteMessageCommand,
	GetQueueAttributesCommand,
	type Message,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SendMessageCommand,
} from "@aws-sdk/client-sqs";
import type { DedupStore } from "../dedupStore.js";
import {
	closeRabbitMqConnections,
	RabbitMqQueueClient,
} from "../RabbitMqQueueClient.js";
import { readRabbitMqBackendConfig } from "../topology.js";

const run = Date.now().toString(36);
const cfg = { ...readRabbitMqBackendConfig(), fifoPartitions: 4, deliveryLimit: 2 };
let failures = 0;
const check = (cond: unknown, label: string) => {
	console.log(`${cond ? "PASS" : "FAIL"}  ${label}`);
	if (!cond) failures++;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

class MemoryDedup implements DedupStore {
	seen = new Set<string>();
	async claim({ queueName, dedupId }: { queueName: string; dedupId: string }) {
		const k = `${queueName}:${dedupId}`;
		if (this.seen.has(k)) return false;
		this.seen.add(k);
		return true;
	}
}

const receiveAll = async (
	client: RabbitMqQueueClient,
	{ max, waitSeconds = 2, visibility = 30 }: { max: number; waitSeconds?: number; visibility?: number },
): Promise<Message[]> => {
	const out: Message[] = [];
	while (out.length < max) {
		const res = (await client.send(
			new ReceiveMessageCommand({
				QueueUrl: "x",
				MaxNumberOfMessages: 10,
				WaitTimeSeconds: waitSeconds,
				VisibilityTimeout: visibility,
			}),
		)) as { Messages?: Message[] };
		if (!res.Messages?.length) break;
		out.push(...res.Messages);
	}
	return out;
};

const ack = (client: RabbitMqQueueClient, msgs: Message[]) =>
	client.send(
		new DeleteMessageBatchCommand({
			QueueUrl: "x",
			Entries: msgs.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })),
		}),
	) as Promise<{ Successful?: unknown[]; Failed?: unknown[] }>;

// ------------------------------------------------------------------ FIFO
{
	const fifoUrl = `rabbitmq://e2e-${run}-primary.fifo`;
	const dedup = new MemoryDedup();
	const producer = new RabbitMqQueueClient({ queueUrl: fifoUrl, config: cfg, dedupStore: dedup });
	const consumer = new RabbitMqQueueClient({ queueUrl: fifoUrl, config: cfg, dedupStore: dedup });

	// 3 groups × 15 messages, interleaved, via SendMessageBatch (as SqsBatchAccumulator does)
	const groups = ["org_a", "org_b", "org_c"];
	const entries = [];
	for (let i = 0; i < 15; i++) {
		for (const g of groups) {
			entries.push({
				Id: `${g}-${i}`,
				MessageBody: JSON.stringify({ name: "Track", data: { g, i } }),
				MessageGroupId: g,
				MessageDeduplicationId: `${run}-${g}-${i}`,
			});
		}
	}
	for (let i = 0; i < entries.length; i += 10) {
		const res = (await producer.send(
			new SendMessageBatchCommand({ QueueUrl: "x", Entries: entries.slice(i, i + 10) }),
		)) as { Successful?: unknown[]; Failed?: unknown[] };
		check((res.Successful?.length ?? 0) === Math.min(10, entries.length - i) && !res.Failed?.length, `batch send ${i / 10 + 1} accepted`);
	}

	// duplicate dedup id → accepted, not enqueued
	const dup = (await producer.send(
		new SendMessageCommand({
			QueueUrl: "x",
			MessageBody: "dup",
			MessageGroupId: "org_a",
			MessageDeduplicationId: `${run}-org_a-0`,
		}),
	)) as { MessageId?: string };
	check(Boolean(dup.MessageId), "dedup'd send still returns a MessageId (SQS semantics)");

	// Only one message per partition should be visible at a time (prefetch 1 + SAC).
	const first = (await consumer.send(
		new ReceiveMessageCommand({ QueueUrl: "x", MaxNumberOfMessages: 10, WaitTimeSeconds: 3, VisibilityTimeout: 30 }),
	)) as { Messages?: Message[] };
	check((first.Messages?.length ?? 0) <= cfg.fifoPartitions, `in-flight bounded by partitions (${first.Messages?.length} <= ${cfg.fifoPartitions})`);

	// Drain everything, acking as we go, and record per-group order.
	const seen: Record<string, number[]> = {};
	let all = [...(first.Messages ?? [])];
	await ack(consumer, all);
	for (;;) {
		const batch = await receiveAll(consumer, { max: 10, waitSeconds: 2 });
		if (!batch.length) break;
		all.push(...batch);
		await ack(consumer, batch);
	}
	for (const m of all) {
		const { g, i } = JSON.parse(m.Body!).data;
		(seen[g] ??= []).push(i);
	}
	check(all.length === 45, `received exactly 45 (got ${all.length}; dedup'd dup excluded)`);
	for (const g of groups) {
		const ordered = seen[g]?.every((v, idx) => idx === 0 || v > seen[g][idx - 1]);
		check(ordered && seen[g]?.length === 15, `group ${g} strictly ordered, 15 msgs`);
	}
	check(all.every((m) => m.Attributes?.ApproximateReceiveCount === "1" && Number(m.Attributes?.SentTimestamp) > 0), "attributes populated, first delivery count=1");

	producer.destroy();
	consumer.destroy();
}

// --------------------------------------------------- visibility + DLQ + delay
{
	const stdUrl = `rabbitmq://e2e-${run}-std`;
	const client = new RabbitMqQueueClient({ queueUrl: stdUrl, config: cfg });

	await client.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: "vis" }));
	let [m] = await receiveAll(client, { max: 1, visibility: 1 });
	check(m?.Body === "vis" && m.Attributes?.ApproximateReceiveCount === "1", "std receive #1");
	// don't delete: visibility 1s → redelivered with count 2
	[m] = await receiveAll(client, { max: 1, waitSeconds: 4, visibility: 1 });
	check(m?.Attributes?.ApproximateReceiveCount === "2", `redelivered after visibility timeout (count=${m?.Attributes?.ApproximateReceiveCount})`);
	// extend visibility, make sure it does NOT come back inside the window
	await client.send(new ChangeMessageVisibilityCommand({ QueueUrl: "x", ReceiptHandle: m.ReceiptHandle!, VisibilityTimeout: 5 }));
	const none = await receiveAll(client, { max: 1, waitSeconds: 2, visibility: 1 });
	check(none.length === 0, "ChangeMessageVisibility extended the lock");
	// let it expire a 3rd time → exceeds delivery limit 2 → DLQ
	await sleep(4000);
	const gone = await receiveAll(client, { max: 1, waitSeconds: 3, visibility: 1 });
	check(gone.length === 0, "message past x-delivery-limit no longer delivered");
	const dlq = await Bun.$`rabbitmqctl list_queues name messages -q`.text();
	check(new RegExp(`e2e-${run}-std\\.dlq\\s+1`).test(dlq), "dead-lettered into <Q>.dlq");

	// delay: 1s bucket → not visible immediately, visible after ~1s
	const t0 = Date.now();
	await client.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: "delayed", DelaySeconds: 1 }));
	const early = await receiveAll(client, { max: 1, waitSeconds: 0 });
	check(early.length === 0, "delayed message not visible at t=0");
	const [late] = await receiveAll(client, { max: 1, waitSeconds: 4 });
	check(late?.Body === "delayed" && Date.now() - t0 >= 900, `delayed message arrived after ${Date.now() - t0}ms`);
	await client.send(new DeleteMessageCommand({ QueueUrl: "x", ReceiptHandle: late.ReceiptHandle! }));

	// invalid handle → sender fault
	const bad = (await client.send(
		new DeleteMessageBatchCommand({ QueueUrl: "x", Entries: [{ Id: "z", ReceiptHandle: "nope" }] }),
	)) as { Failed?: { SenderFault?: boolean }[] };
	check(bad.Failed?.[0]?.SenderFault === true, "invalid receipt handle reported as SenderFault");

	// attributes
	await client.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: "a" }));
	await client.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: "b" }));
	await sleep(300);
	const attrs = (await client.send(
		new GetQueueAttributesCommand({ QueueUrl: "x", AttributeNames: ["ApproximateNumberOfMessages"] }),
	)) as { Attributes?: Record<string, string> };
	// consumer prefetch may already hold them locally; ready + local in-flight-not-handed-out should be 2
	check(["0", "1", "2"].includes(attrs.Attributes?.ApproximateNumberOfMessages ?? ""), `GetQueueAttributes answers (ready=${attrs.Attributes?.ApproximateNumberOfMessages})`);

	client.destroy();
}

await sleep(200);
await closeRabbitMqConnections();
console.log(failures ? `\n${failures} FAILURE(S)` : "\nALL PASSED");
process.exit(failures ? 1 : 0);
