import { createHash } from "crypto";
import Redis from "ioredis";
import logger from "./logger.ts";

interface QueueWaiter {
  resolve: (release: () => Promise<void>) => void;
  reject: (error: Error) => void;
  timeoutHandle: ReturnType<typeof setTimeout> | null;
}

interface SessionBucket {
  active: number;
  lastStartedAt: number;
  queue: QueueWaiter[];
  timer: ReturnType<typeof setTimeout> | null;
}

function parseIntWithDefault(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function maskSession(sessionId: string): string {
  if (!sessionId) return "unknown";
  if (sessionId.length <= 10) return "***";
  return `${sessionId.slice(0, 4)}***${sessionId.slice(-4)}`;
}

class SessionRateLimiter {
  private enabled: boolean;
  private minIntervalMs: number;
  private maxConcurrent: number;
  private maxQueuePerSession: number;
  private queueTimeoutMs: number;
  private useRedisDistributed: boolean;
  private redisUrl: string;
  private inflightTtlMs: number;
  private redisPollIntervalMs: number;
  private redis: any = null;
  private redisInitPromise: Promise<any | null> | null = null;
  private buckets: Map<string, SessionBucket> = new Map();
  private static readonly REDIS_KEY_PREFIX = "jimeng:session_rl";
  private static readonly REDIS_ACQUIRE_SCRIPT = `
local inflightKey = KEYS[1]
local intervalKey = KEYS[2]

local nowMs = tonumber(ARGV[1])
local minIntervalMs = tonumber(ARGV[2])
local maxConcurrent = tonumber(ARGV[3])
local inflightTtlMs = tonumber(ARGV[4])
local intervalTtlMs = tonumber(ARGV[5])

local inflight = tonumber(redis.call("GET", inflightKey) or "0")
if inflight >= maxConcurrent then
  return {0, "concurrency", 0, inflight}
end

local nextAllowedAt = tonumber(redis.call("GET", intervalKey) or "0")
if nextAllowedAt > nowMs then
  return {0, "interval", nextAllowedAt - nowMs, inflight}
end

redis.call("SET", intervalKey, tostring(nowMs + minIntervalMs), "PX", intervalTtlMs)
local newInflight = redis.call("INCR", inflightKey)
redis.call("PEXPIRE", inflightKey, inflightTtlMs)

return {1, "ok", 0, newInflight}
`;
  private static readonly REDIS_RELEASE_SCRIPT = `
local inflightKey = KEYS[1]
local inflight = tonumber(redis.call("GET", inflightKey) or "0")
if inflight <= 1 then
  redis.call("DEL", inflightKey)
  return 0
end
return redis.call("DECR", inflightKey)
`;

  constructor() {
    this.enabled = process.env.SESSION_RATE_LIMIT_ENABLED !== "false";
    this.minIntervalMs = Math.max(0, parseIntWithDefault(process.env.SESSION_MIN_INTERVAL_MS, 200));
    this.maxConcurrent = Math.max(1, parseIntWithDefault(process.env.SESSION_MAX_CONCURRENT, 20));
    this.maxQueuePerSession = Math.max(1, parseIntWithDefault(process.env.SESSION_MAX_QUEUE_PER_SESSION, 2000));
    this.queueTimeoutMs = Math.max(1000, parseIntWithDefault(process.env.SESSION_QUEUE_TIMEOUT_MS, 120000));
    this.redisUrl = (process.env.REDIS_URL || "").trim();
    this.useRedisDistributed = process.env.SESSION_RATE_LIMIT_DISTRIBUTED !== "false" && this.redisUrl.length > 0;
    this.inflightTtlMs = Math.max(10000, parseIntWithDefault(process.env.SESSION_INFLIGHT_TTL_MS, 120000));
    this.redisPollIntervalMs = Math.max(20, parseIntWithDefault(process.env.SESSION_REDIS_POLL_INTERVAL_MS, 60));

    logger.info(
      `[SessionRateLimiter] enabled=${this.enabled}, distributed=${this.useRedisDistributed}, minIntervalMs=${this.minIntervalMs}, maxConcurrent=${this.maxConcurrent}, maxQueuePerSession=${this.maxQueuePerSession}, queueTimeoutMs=${this.queueTimeoutMs}, inflightTtlMs=${this.inflightTtlMs}`
    );
  }

  async acquire(sessionId: string): Promise<() => Promise<void>> {
    if (!this.enabled || !sessionId) {
      return async () => {};
    }

    const redis = await this.getRedisClient();
    if (redis) {
      try {
        return await this.acquireDistributed(redis, sessionId);
      } catch (err: any) {
        logger.warn(`[SessionRateLimiter] distributed acquire failed, fallback to local: ${err.message}`);
      }
    }

    return this.acquireLocal(sessionId);
  }

  private async getRedisClient(): Promise<any | null> {
    if (!this.useRedisDistributed) return null;
    if (this.redis) return this.redis;
    if (this.redisInitPromise) return this.redisInitPromise;

    this.redisInitPromise = (async () => {
      try {
        const client = new Redis(this.redisUrl, {
          lazyConnect: true,
          maxRetriesPerRequest: 2,
          retryStrategy(times) {
            if (times > 4) return null;
            return Math.min(times * 300, 1500);
          },
        });

        client.on("error", (err: any) => {
          logger.warn(`[SessionRateLimiter] redis error: ${err.message}`);
        });

        await client.connect();
        logger.info("[SessionRateLimiter] distributed mode enabled (Redis connected)");
        this.redis = client;
        return this.redis;
      } catch (err: any) {
        logger.warn(`[SessionRateLimiter] redis unavailable, fallback to local mode: ${err.message}`);
        this.redis = null;
        return null;
      } finally {
        this.redisInitPromise = null;
      }
    })();

    return this.redisInitPromise;
  }

  private getRedisKeys(sessionId: string): { inflightKey: string; intervalKey: string } {
    const digest = createHash("sha1").update(sessionId).digest("hex");
    return {
      inflightKey: `${SessionRateLimiter.REDIS_KEY_PREFIX}:inflight:${digest}`,
      intervalKey: `${SessionRateLimiter.REDIS_KEY_PREFIX}:next:${digest}`,
    };
  }

  private async acquireDistributed(redis: any, sessionId: string): Promise<() => Promise<void>> {
    const startedAt = Date.now();
    const deadlineAt = startedAt + this.queueTimeoutMs;
    const { inflightKey, intervalKey } = this.getRedisKeys(sessionId);
    const intervalTtlMs = Math.max(this.minIntervalMs * 50, 60000);

    while (true) {
      const now = Date.now();
      if (now >= deadlineAt) {
        throw new Error(`[SessionRateLimiter] wait timeout for ${maskSession(sessionId)} (${this.queueTimeoutMs}ms)`);
      }

      const result = await redis.eval(
        SessionRateLimiter.REDIS_ACQUIRE_SCRIPT,
        2,
        inflightKey,
        intervalKey,
        String(now),
        String(this.minIntervalMs),
        String(this.maxConcurrent),
        String(this.inflightTtlMs),
        String(intervalTtlMs)
      );

      const granted = Number(result?.[0] || 0) === 1;
      const reason = String(result?.[1] || "");
      const waitMs = Math.max(0, Number(result?.[2] || 0));

      if (granted) {
        let released = false;
        return async () => {
          if (released) return;
          released = true;
          try {
            await redis.eval(SessionRateLimiter.REDIS_RELEASE_SCRIPT, 1, inflightKey);
          } catch (err: any) {
            logger.warn(`[SessionRateLimiter] distributed release failed: ${err.message}`);
          }
        };
      }

      const leftMs = deadlineAt - Date.now();
      if (leftMs <= 0) {
        throw new Error(`[SessionRateLimiter] wait timeout for ${maskSession(sessionId)} (${this.queueTimeoutMs}ms)`);
      }

      if (reason === "interval" && waitMs > 0) {
        await this.sleep(Math.min(waitMs, leftMs));
        continue;
      }

      const backoffMs = Math.min(this.redisPollIntervalMs + Math.floor(Math.random() * 40), leftMs);
      await this.sleep(backoffMs);
    }
  }

  private async acquireLocal(sessionId: string): Promise<() => Promise<void>> {
    const bucket = this.getBucket(sessionId);
    if (bucket.queue.length >= this.maxQueuePerSession) {
      throw new Error(
        `[SessionRateLimiter] session queue overflow for ${maskSession(sessionId)} (limit=${this.maxQueuePerSession})`
      );
    }

    return new Promise((resolve, reject) => {
      const waiter: QueueWaiter = {
        resolve,
        reject,
        timeoutHandle: null,
      };

      if (this.queueTimeoutMs > 0) {
        waiter.timeoutHandle = setTimeout(() => {
          const index = bucket.queue.indexOf(waiter);
          if (index === -1) return;
          bucket.queue.splice(index, 1);
          waiter.reject(
            new Error(
              `[SessionRateLimiter] wait timeout for ${maskSession(sessionId)} (${this.queueTimeoutMs}ms)`
            )
          );
          this.cleanupBucket(sessionId, bucket);
        }, this.queueTimeoutMs);
      }

      bucket.queue.push(waiter);
      this.processQueue(sessionId, bucket);
    });
  }

  private getBucket(sessionId: string): SessionBucket {
    const existing = this.buckets.get(sessionId);
    if (existing) return existing;

    const created: SessionBucket = {
      active: 0,
      lastStartedAt: 0,
      queue: [],
      timer: null,
    };
    this.buckets.set(sessionId, created);
    return created;
  }

  private processQueue(sessionId: string, bucket: SessionBucket): void {
    if (!this.enabled) return;
    if (bucket.active >= this.maxConcurrent) return;
    if (bucket.queue.length === 0) {
      this.cleanupBucket(sessionId, bucket);
      return;
    }

    const now = Date.now();
    const waitMs = Math.max(0, bucket.lastStartedAt + this.minIntervalMs - now);
    if (waitMs > 0) {
      if (!bucket.timer) {
        bucket.timer = setTimeout(() => {
          bucket.timer = null;
          this.processQueue(sessionId, bucket);
        }, waitMs);
      }
      return;
    }

    const waiter = bucket.queue.shift();
    if (!waiter) return;
    if (waiter.timeoutHandle) clearTimeout(waiter.timeoutHandle);

    bucket.active += 1;
    bucket.lastStartedAt = Date.now();

    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      bucket.active = Math.max(0, bucket.active - 1);
      this.processQueue(sessionId, bucket);
      this.cleanupBucket(sessionId, bucket);
    };

    waiter.resolve(release);

    if (bucket.active < this.maxConcurrent) {
      this.processQueue(sessionId, bucket);
    }
  }

  private cleanupBucket(sessionId: string, bucket: SessionBucket): void {
    if (bucket.active > 0) return;
    if (bucket.queue.length > 0) return;
    if (bucket.timer) return;
    this.buckets.delete(sessionId);
  }

  private async sleep(ms: number): Promise<void> {
    if (ms <= 0) return;
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}

const sessionRateLimiter = new SessionRateLimiter();
export default sessionRateLimiter;
