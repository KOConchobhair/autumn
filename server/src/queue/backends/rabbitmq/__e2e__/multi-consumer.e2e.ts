import { ReceiveMessageCommand, SendMessageCommand, DeleteMessageCommand, type Message } from "@aws-sdk/client-sqs";
import { RabbitMqQueueClient, closeRabbitMqConnections } from "../RabbitMqQueueClient.ts";
import { readRabbitMqBackendConfig } from "../topology.ts";
const cfg = { ...readRabbitMqBackendConfig(), fifoPartitions: 12 };
const url = `rabbitmq://multi-${Date.now().toString(36)}.fifo`;
const producer = new RabbitMqQueueClient({ queueUrl: url, config: cfg });
const consumers = [0, 1, 2].map(() => new RabbitMqQueueClient({ queueUrl: url, config: cfg }));
const groups = ["a", "b", "c", "d", "e", "f"]; // 6 groups over 3 partitions
for (let i = 0; i < 20; i++) for (const g of groups)
  await producer.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: JSON.stringify({ g, i }), MessageGroupId: g, MessageDeduplicationId: `${url}-${g}-${i}` }));

const log: { g: string; i: number; c: number }[] = [];
let inflightPeak = 0, inflight = 0, done = 0;
const total = groups.length * 20;
const worker = async (idx: number) => {
  const c = consumers[idx];
  while (done < total) {
    const res = (await c.send(new ReceiveMessageCommand({ QueueUrl: "x", MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30 }))) as { Messages?: Message[] };
    for (const m of res.Messages ?? []) {
      inflight++; inflightPeak = Math.max(inflightPeak, inflight);
      await new Promise((r) => setTimeout(r, Math.random() * 15)); // simulate work, let others race
      const { g, i } = JSON.parse(m.Body!); log.push({ g, i, c: idx });
      await c.send(new DeleteMessageCommand({ QueueUrl: "x", ReceiptHandle: m.ReceiptHandle! }));
      inflight--; done++;
    }
  }
};
await Promise.all(consumers.map((_, i) => worker(i)));
const byGroup: Record<string, number[]> = {};
for (const e of log) (byGroup[e.g] ??= []).push(e.i);
const ordered = Object.values(byGroup).every((arr) => arr.every((v, k) => k === 0 || v === arr[k - 1] + 1));
const perConsumer = consumers.map((_, i) => log.filter((e) => e.c === i).length);
console.log(`received=${log.length}/${total} ordered_per_group=${ordered} inflight_peak=${inflightPeak} (partitions=${cfg.fifoPartitions}) per_consumer=${JSON.stringify(perConsumer)}`);
producer.destroy(); consumers.forEach((c) => c.destroy());
await new Promise((r) => setTimeout(r, 200)); await closeRabbitMqConnections();
process.exit(log.length === total && ordered && inflightPeak <= cfg.fifoPartitions ? 0 : 1);
