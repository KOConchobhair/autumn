import {
	type StripeDiscountWithCoupon,
	stripeToAtmnAmount,
} from "@autumn/shared";
import type { BillingChangeFacts } from "@autumn/shared/api/approvals/evaluate/index.js";

/**
 * Discounts this request adds. Ones already on the subscription carry an id
 * and are left out, so an old coupon can't fail a rule on an unrelated change.
 */
export const stripeDiscountsToDiscountFacts = ({
	stripeDiscounts = [],
	currency,
}: {
	stripeDiscounts?: StripeDiscountWithCoupon[];
	currency: string;
}): BillingChangeFacts["discounts"] =>
	stripeDiscounts
		.filter((discount) => !discount.id)
		.map(({ source: { coupon } }) => ({
			reward_id: coupon.id,
			percent_off: coupon.percent_off ?? null,
			amount_off:
				coupon.amount_off === null
					? null
					: stripeToAtmnAmount({
							amount: coupon.amount_off,
							currency: coupon.currency ?? currency,
						}),
		}));
