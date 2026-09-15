import type Redis from "ioredis";
import { DEDUP_WINDOW_MS } from "./topology.js";

/**
 * SQS FIFO drops a message whose MessageDeduplicationId was seen in the last
 * 5 minutes and reports it as accepted. RabbitMQ has no equivalent, so the
 * backend claims the id here before publishing.
 */
export interface DedupStore {
	/** True if the id was NOT seen in the window (caller should publish). */
	claim(params: { queueName: string; dedupId: string }): Promise<boolean>;
}

export class RedisDedupStore implements DedupStore {
	constructor(
		private readonly getRedis: () => Redis,
		private readonly windowMs: number = DEDUP_WINDOW_MS,
	) {}

	async claim({
		queueName,
		dedupId,
	}: {
		queueName: string;
		dedupId: string;
	}): Promise<boolean> {
		const key = `qdedup:${queueName}:${dedupId}`;
		try {
			const result = await this.getRedis().set(key, "1", "PX", this.windowMs, "NX");
			return result === "OK";
		} catch (error) {
			// Fail open: a Redis blip must not stop billing jobs from enqueueing.
			// Handlers are idempotent at the job level; dedup here is a belt.
			console.warn(
				`[RabbitMQ] dedup store unavailable, publishing without dedup (${error instanceof Error ? error.message : error})`,
			);
			return true;
		}
	}
}

/** Used when no Redis is wired (tests) — never dedups. */
export class NoopDedupStore implements DedupStore {
	async claim(): Promise<boolean> {
		return true;
	}
}
