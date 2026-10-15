import type { AppEnv } from "@autumn/shared";
import { logger } from "@/external/logtail/logtailUtils.js";
import {
	forEachMiscRedisTarget,
	resolveMiscRedis,
} from "@/external/redis/miscCache/resolveMiscRedis.js";
import { tryRedisOp } from "@/external/redis/utils/runRedisOp.js";

// Short, because a clear that fails on one instance leaves stale permissions until expiry.
export const APPROVAL_RULES_CACHE_TTL_SECONDS = 60 * 5;

export const buildApprovalRulesCacheKey = ({
	orgId,
	env,
}: {
	orgId: string;
	env: AppEnv;
}) => `approval_rules:${orgId}:${env}`;

export const getCachedApprovalRules = async <T>({
	orgId,
	env,
	requestId,
}: {
	orgId: string;
	env: AppEnv;
	requestId?: string;
}): Promise<T | null> => {
	const redis = resolveMiscRedis({ requestId });
	const cached = await tryRedisOp({
		operation: () => redis.get(buildApprovalRulesCacheKey({ orgId, env })),
		source: "approval-rules-cache:get",
		redisInstance: redis,
	});
	return cached ? (JSON.parse(cached) as T) : null;
};

export const setCachedApprovalRules = async ({
	orgId,
	env,
	requestId,
	value,
}: {
	orgId: string;
	env: AppEnv;
	requestId?: string;
	value: unknown;
}): Promise<void> => {
	const redis = resolveMiscRedis({ requestId });
	await tryRedisOp({
		operation: () =>
			redis.set(
				buildApprovalRulesCacheKey({ orgId, env }),
				JSON.stringify(value),
				"EX",
				APPROVAL_RULES_CACHE_TTL_SECONDS,
			),
		source: "approval-rules-cache:set",
		redisInstance: redis,
	});
};

/** Clears on every live instance, so a rule change applies to the next request wherever it lands. */
export const clearApprovalRulesCache = async ({
	orgId,
	env,
}: {
	orgId: string;
	env: AppEnv;
}): Promise<void> => {
	await forEachMiscRedisTarget({
		operation: ({ redis }) =>
			tryRedisOp({
				operation: () => redis.del(buildApprovalRulesCacheKey({ orgId, env })),
				source: "approval-rules-cache:clear",
				redisInstance: redis,
			}),
		onError: ({ target }) => {
			logger.warn(
				`[approvalRulesCache] clear failed on "${target.instanceName}" (org: ${orgId}, env: ${env})`,
			);
		},
	});
};
