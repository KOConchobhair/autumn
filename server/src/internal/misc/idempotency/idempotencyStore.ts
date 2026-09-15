import { claimDynamoIdempotencyKey } from "@/external/aws/dynamodb/idempotencyKeys/operations/claimDynamoIdempotencyKey.js";
import { releaseDynamoIdempotencyKey } from "@/external/aws/dynamodb/idempotencyKeys/operations/releaseDynamoIdempotencyKey.js";
import {
	claimRedisIdempotencyKey,
	releaseRedisIdempotencyKey,
} from "@/external/redis/idempotencyKeys/redisIdempotencyKey.js";

/** `IDEMPOTENCY_BACKEND=redis` swaps DynamoDB for the misc Redis. Default
 *  stays DynamoDB so upstream behaviour is unchanged unless opted in. */
const useRedisBackend = (): boolean =>
	process.env.IDEMPOTENCY_BACKEND === "redis";

export const claimIdempotencyKey: typeof claimDynamoIdempotencyKey = (params) =>
	useRedisBackend()
		? claimRedisIdempotencyKey(params)
		: claimDynamoIdempotencyKey(params);

export const releaseIdempotencyKeyInStore: typeof releaseDynamoIdempotencyKey =
	(params) =>
		useRedisBackend()
			? releaseRedisIdempotencyKey(params)
			: releaseDynamoIdempotencyKey(params);
