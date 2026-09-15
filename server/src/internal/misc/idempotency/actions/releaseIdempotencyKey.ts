import type { AutumnContext } from "@/honoUtils/HonoEnv.js";
import { buildIdempotencyStorageKey } from "../idempotencyKeyUtils.js";
import { releaseIdempotencyKeyInStore } from "../idempotencyStore.js";

/** Frees the key so the client can retry. Swallows its own errors — a failed
 *  release only costs the caller a retry window, never the request. */
export const releaseIdempotencyKey = async ({
	ctx,
	idempotencyKey,
}: {
	ctx: AutumnContext;
	idempotencyKey: string;
}): Promise<void> => {
	const { storageKey } = buildIdempotencyStorageKey({
		orgId: ctx.org.id,
		env: ctx.env,
		idempotencyKey,
	});

	await releaseIdempotencyKeyInStore({ storageKey });
};
