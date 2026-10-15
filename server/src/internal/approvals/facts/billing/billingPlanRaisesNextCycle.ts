import type { BillingContext, BillingPlan } from "@autumn/shared";
import type { AutumnContext } from "@/honoUtils/HonoEnv";
import { billingPlanToNextCyclePreview } from "@/internal/billing/v2/utils/billingPlan/toNextCyclePreview/billingPlanToNextCyclePreview";

const nextCycleTotal = ({
	ctx,
	billingContext,
	billingPlan,
}: {
	ctx: AutumnContext;
	billingContext: BillingContext;
	billingPlan: BillingPlan;
}) =>
	billingPlanToNextCyclePreview({ ctx, billingContext, billingPlan }).nextCycle
		?.total ?? 0;

/**
 * Whether the next cycle bills more than it would without this change. Both
 * sides are pre-tax, so tax alone never reads as a new charge.
 */
export const billingPlanRaisesNextCycle = ({
	ctx,
	billingContext,
	billingPlan,
}: {
	ctx: AutumnContext;
	billingContext: BillingContext;
	billingPlan: BillingPlan;
}): boolean => {
	const unchangedPlan: BillingPlan = {
		autumn: {
			customerId: billingPlan.autumn.customerId,
			insertCustomerProducts: [],
		},
		stripe: {},
	};
	return (
		nextCycleTotal({ ctx, billingContext, billingPlan }) >
		nextCycleTotal({ ctx, billingContext, billingPlan: unchangedPlan })
	);
};
