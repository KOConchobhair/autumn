import { getMiscMainRedis } from "@/external/redis/miscCache/miscRedisInstances.js";
import type { Logger } from "@/external/logtail/logtailUtils";
import {
	IDEMPOTENCY_TTL_MS,
	type IdempotencyClaimResult,
} from "@/internal/misc/idempotency/idempotencyKeyUtils.js";

/**
 * Redis-backed idempotency claims — same contract as the DynamoDB operations
 * (`claimed` | `duplicate` | `unavailable`), selected with
 * `IDEMPOTENCY_BACKEND=redis`. `SET NX PX` gives the conditional-put +
 * TTL pair DynamoDB provided in one atomic command, and Redis expiry is
 * exact, so the "expired but present" branch DynamoDB needed goes away.
 *
 * Runs on the misc Redis, which is already a hard dependency; there is no
 * new failure domain. Persistence (AOF) on that instance is what makes a
 * claim survive a failover.
 */

const KEY_PREFIX = "idem:";

export const claimRedisIdempotencyKey = async ({
	storageKey,
	ttlMs = IDEMPOTENCY_TTL_MS,
	logger,
}: {
	storageKey: string;
	ttlMs?: number;
	logger?: Logger;
}): Promise<IdempotencyClaimResult> => {
	try {
		const result = await getMiscMainRedis().set(
			`${KEY_PREFIX}${storageKey}`,
			String(Date.now()),
			"PX",
			Math.max(1000, Math.floor(ttlMs)),
			"NX",
		);
		return result === "OK" ? "claimed" : "duplicate";
	} catch (error) {
		logger?.warn(
			`[claimRedisIdempotencyKey] redis unavailable, failing open: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);
		return "unavailable";
	}
};

export const releaseRedisIdempotencyKey = async ({
	storageKey,
}: {
	storageKey: string;
}): Promise<void> => {
	try {
		await getMiscMainRedis().del(`${KEY_PREFIX}${storageKey}`);
	} catch {
		// A failed release only costs the caller a retry window.
	}
};
