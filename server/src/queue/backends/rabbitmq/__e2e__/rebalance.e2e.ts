import { DeleteMessageCommand, type Message, ReceiveMessageCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { closeRabbitMqConnections, RabbitMqQueueClient } from "../RabbitMqQueueClient.js";
import { readRabbitMqBackendConfig } from "../topology.js";

// Fast timers for the test; production defaults are 15s / 60s.
const cfg = { ...readRabbitMqBackendConfig(), fifoPartitions: 12, rebalanceIntervalMs: 2_000, releaseCooldownMs: 8_000 };
const url = `rabbitmq://rebal-${Date.now().toString(36)}.fifo`;
const producer = new RabbitMqQueueClient({ queueUrl: url, config: cfg });
const groups = Array.from({ length: 24 }, (_, i) => `g${i}`);

// Consumer 0 subscribes alone first (simulates the oldest pod owning everything).
const consumers = [new RabbitMqQueueClient({ queueUrl: url, config: cfg })];
const counts = [0, 0, 0];
const windowCounts = [0, 0, 0];
let stop = false;
const drain = async (idx: number) => {
  const c = consumers[idx];
  while (!stop) {
    const res = (await c.send(new ReceiveMessageCommand({ QueueUrl: "x", MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30 }))) as { Messages?: Message[] };
    for (const m of res.Messages ?? []) {
      counts[idx]++; windowCounts[idx]++;
      await c.send(new DeleteMessageCommand({ QueueUrl: "x", ReceiptHandle: m.ReceiptHandle! }));
    }
  }
};
void drain(0);
await new Promise((r) => setTimeout(r, 1500));
// Late joiners
consumers.push(new RabbitMqQueueClient({ queueUrl: url, config: cfg }), new RabbitMqQueueClient({ queueUrl: url, config: cfg }));
void drain(1); void drain(2);

// Steady trickle across all groups for ~30s
const t0 = Date.now();
let seq = 0;
const producerLoop = (async () => {
  while (Date.now() - t0 < 30_000) {
    for (const g of groups) await producer.send(new SendMessageCommand({ QueueUrl: "x", MessageBody: `${g}:${seq}`, MessageGroupId: g, MessageDeduplicationId: `${url}-${g}-${seq}` }));
    seq++;
    await new Promise((r) => setTimeout(r, 400));
  }
})();
const snapshots: string[] = [];
const snap = setInterval(() => { snapshots.push(`t=${Math.round((Date.now() - t0) / 1000)}s last5s=${JSON.stringify(windowCounts)}`); windowCounts.fill(0); }, 5000);
await producerLoop;
await new Promise((r) => setTimeout(r, 3000));
stop = true; clearInterval(snap);
console.log(snapshots.join("\n"));
console.log(`total=${JSON.stringify(counts)} sent=${seq * groups.length} received=${counts.reduce((a, b) => a + b, 0)}`);
const lateShare = (counts[1] + counts[2]) / counts.reduce((a, b) => a + b, 0);
console.log(`late-joiner share=${(lateShare * 100).toFixed(0)}% (want > 40%)`);
producer.destroy(); consumers.forEach((c) => c.destroy());
await new Promise((r) => setTimeout(r, 300)); await closeRabbitMqConnections();
process.exit(lateShare > 0.4 && counts.reduce((a, b) => a + b, 0) === seq * groups.length ? 0 : 1);
