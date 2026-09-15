import pg from "pg";

/**
 * Postgres advisory-lock leader election for the cron process.
 *
 * Upstream's only guard against two cron replicas double-firing is the S3
 * blue-green slot, which fails open off AWS. With `CRON_LEADER_LOCK=postgres`
 * every cron gate (`isActiveSlot({ serviceName: "cron" })`) additionally
 * requires this process to hold `pg_try_advisory_lock(CRON_LEADER_LOCK_KEY)`.
 *
 * Session advisory locks live on one connection, so this uses a dedicated
 * `pg.Client` against a DIRECT Postgres port — never PgBouncer in transaction
 * mode. `CRON_LEADER_LOCK_DATABASE_URL` overrides; default is
 * `DATABASE_CRITICAL_URL`, then `DATABASE_URL`.
 *
 * Losing the connection drops leadership immediately (the server releases
 * the lock when the session dies), so a network partition can't leave two
 * leaders for longer than the follower's poll interval.
 */

const DEFAULT_LOCK_KEY = 0x6175746d; // "autm"
const POLL_MS = 5_000;

let client: pg.Client | null = null;
let leader = false;
let stopped = true;
let timer: ReturnType<typeof setTimeout> | null = null;

export const isCronLeaderLockEnabled = (): boolean =>
	process.env.CRON_LEADER_LOCK === "postgres";

/** True when this process may run cron work. Always true when disabled. */
export const isCronLeader = (): boolean => !isCronLeaderLockEnabled() || leader;

const lockKey = (): number => {
	const parsed = Number.parseInt(process.env.CRON_LEADER_LOCK_KEY ?? "", 10);
	return Number.isFinite(parsed) ? parsed : DEFAULT_LOCK_KEY;
};

const connectionString = (): string => {
	const url =
		process.env.CRON_LEADER_LOCK_DATABASE_URL ||
		process.env.DATABASE_CRITICAL_URL ||
		process.env.DATABASE_URL;
	if (!url) throw new Error("[cronLeaderLock] no database URL configured");
	return url;
};

const dropLeadership = (reason: string) => {
	if (leader) console.warn(`[cronLeaderLock] leadership lost: ${reason}`);
	leader = false;
	const c = client;
	client = null;
	if (c) {
		c.removeAllListeners();
		void c.end().catch(() => undefined);
	}
};

const tick = async () => {
	if (stopped) return;
	try {
		if (!client) {
			const next = new pg.Client({
				connectionString: connectionString(),
				application_name: "autumn-cron-leader-lock",
				keepAlive: true,
			});
			next.on("error", (err) => dropLeadership(`connection error: ${err.message}`));
			next.on("end", () => dropLeadership("connection ended"));
			await next.connect();
			client = next;
		}
		if (!leader) {
			const { rows } = await client.query<{ locked: boolean }>(
				"SELECT pg_try_advisory_lock($1) AS locked",
				[lockKey()],
			);
			if (rows[0]?.locked) {
				leader = true;
				console.log(`[cronLeaderLock] acquired leadership (pid ${process.pid})`);
			}
		} else {
			// Cheap liveness probe; a dead session means the lock is gone.
			await client.query("SELECT 1");
		}
	} catch (error) {
		dropLeadership(error instanceof Error ? error.message : String(error));
	} finally {
		if (!stopped) timer = setTimeout(tick, POLL_MS);
	}
};

export const startCronLeaderLock = (): void => {
	if (!isCronLeaderLockEnabled() || !stopped) return;
	stopped = false;
	console.log("[cronLeaderLock] enabled; waiting for advisory lock");
	void tick();
};

export const stopCronLeaderLock = async (): Promise<void> => {
	stopped = true;
	if (timer) clearTimeout(timer);
	timer = null;
	const c = client;
	client = null;
	leader = false;
	if (c) {
		c.removeAllListeners();
		try {
			await c.query("SELECT pg_advisory_unlock_all()");
		} catch {
			// session end releases it anyway
		}
		await c.end().catch(() => undefined);
	}
};
