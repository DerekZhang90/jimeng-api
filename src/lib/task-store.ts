import Redis from "ioredis";
import { v1 as uuid } from "uuid";
import logger from "./logger.ts";

export type TaskStatus = "pending" | "queued" | "processing" | "completed" | "failed" | "cancelled";
export type TaskType = "image" | "video" | "composition";

export interface Task {
  id: string;
  type: TaskType;
  status: TaskStatus;
  progress?: string;
  created_at: number;
  updated_at: number;
  completed_at?: number;
  result?: any;
  error?: string;
  callback_url?: string;
  model?: string;
  prompt?: string;
}

const KEY_PREFIX = "jimeng:task:";
const INDEX_KEY = "jimeng:task_ids";
const EXPIRE_HOURS = parseInt(process.env.TASK_EXPIRE_HOURS || "1", 10);
const EXPIRE_SECONDS = EXPIRE_HOURS * 3600;

class TaskStore {
  private redis: Redis | null = null;
  private memoryStore: Map<string, Task> = new Map();
  private useRedis = false;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;

  async init(): Promise<void> {
    const redisUrl = process.env.REDIS_URL;
    if (!redisUrl) {
      logger.warn("[TaskStore] REDIS_URL 未配置，使用内存模式存储任务（重启将丢失）");
      this.startMemoryCleanup();
      return;
    }

    try {
      this.redis = new Redis(redisUrl, {
        maxRetriesPerRequest: 3,
        retryStrategy(times) {
          if (times > 5) return null;
          return Math.min(times * 500, 3000);
        },
        lazyConnect: true,
      });

      // 监听错误事件，防止未捕获异常
      this.redis.on("error", (err) => {
        if (this.useRedis) {
          logger.error(`[TaskStore] Redis 连接错误: ${err.message}，降级到内存模式`);
          this.useRedis = false;
          this.startMemoryCleanup();
        }
      });

      this.redis.on("connect", () => {
        logger.info("[TaskStore] Redis 连接成功");
      });

      await this.redis.connect();
      this.useRedis = true;
      logger.success("[TaskStore] 使用 Redis 模式存储任务");
    } catch (err: any) {
      logger.warn(`[TaskStore] Redis 连接失败: ${err.message}，降级到内存模式`);
      this.redis = null;
      this.useRedis = false;
      this.startMemoryCleanup();
    }
  }

  private startMemoryCleanup(): void {
    if (this.cleanupTimer) return;
    // 每 5 分钟清理过期任务
    this.cleanupTimer = setInterval(() => {
      const now = Math.floor(Date.now() / 1000);
      const expireThreshold = now - EXPIRE_SECONDS;
      let cleaned = 0;
      for (const [id, task] of this.memoryStore) {
        if (
          task.completed_at &&
          task.completed_at < expireThreshold &&
          (task.status === "completed" || task.status === "failed" || task.status === "cancelled")
        ) {
          this.memoryStore.delete(id);
          cleaned++;
        }
      }
      if (cleaned > 0) {
        logger.debug(`[TaskStore] 内存清理: 移除 ${cleaned} 个过期任务，剩余 ${this.memoryStore.size} 个`);
      }
    }, 5 * 60 * 1000);
  }

  async create(data: Partial<Task>): Promise<Task> {
    const now = Math.floor(Date.now() / 1000);
    const task: Task = {
      id: uuid(),
      type: data.type || "image",
      status: data.status || "pending",
      progress: data.progress,
      created_at: now,
      updated_at: now,
      callback_url: data.callback_url,
      model: data.model,
      prompt: data.prompt,
    };

    if (this.useRedis && this.redis) {
      try {
        await this.redis.set(`${KEY_PREFIX}${task.id}`, JSON.stringify(task));
        await this.redis.sadd(INDEX_KEY, task.id);
        return task;
      } catch (err: any) {
        logger.warn(`[TaskStore] Redis 写入失败: ${err.message}，降级到内存`);
        this.useRedis = false;
        this.startMemoryCleanup();
      }
    }

    this.memoryStore.set(task.id, { ...task });
    return task;
  }

  async get(id: string): Promise<Task | null> {
    if (this.useRedis && this.redis) {
      try {
        const data = await this.redis.get(`${KEY_PREFIX}${id}`);
        return data ? JSON.parse(data) : null;
      } catch (err: any) {
        logger.warn(`[TaskStore] Redis 读取失败: ${err.message}，降级到内存`);
        this.useRedis = false;
        this.startMemoryCleanup();
      }
    }

    const task = this.memoryStore.get(id);
    return task ? { ...task } : null;
  }

  async update(id: string, updates: Partial<Task>): Promise<void> {
    const task = await this.get(id);
    if (!task) return;

    const updated: Task = {
      ...task,
      ...updates,
      id: task.id, // id 不可变
      updated_at: Math.floor(Date.now() / 1000),
    };

    if (this.useRedis && this.redis) {
      try {
        await this.redis.set(`${KEY_PREFIX}${id}`, JSON.stringify(updated));
        // 任务完成/失败/取消时设置过期
        if (updated.status === "completed" || updated.status === "failed" || updated.status === "cancelled") {
          await this.redis.expire(`${KEY_PREFIX}${id}`, EXPIRE_SECONDS);
        }
        return;
      } catch (err: any) {
        logger.warn(`[TaskStore] Redis 更新失败: ${err.message}，降级到内存`);
        this.useRedis = false;
        this.startMemoryCleanup();
      }
    }

    this.memoryStore.set(id, updated);
  }

  async list(options?: { status?: TaskStatus; type?: TaskType; limit?: number }): Promise<Task[]> {
    let tasks: Task[] = [];
    const limit = options?.limit || 100;

    if (this.useRedis && this.redis) {
      try {
        const ids = await this.redis.smembers(INDEX_KEY);
        if (ids.length === 0) return [];

        const pipeline = this.redis.pipeline();
        for (const id of ids) {
          pipeline.get(`${KEY_PREFIX}${id}`);
        }
        const results = await pipeline.exec();

        for (let i = 0; i < results!.length; i++) {
          const [err, data] = results![i];
          if (!err && data) {
            const task = JSON.parse(data as string);
            tasks.push(task);
          } else if (!data) {
            // 键已过期，从索引中移除
            await this.redis.srem(INDEX_KEY, ids[i]);
          }
        }
      } catch (err: any) {
        logger.warn(`[TaskStore] Redis 列表查询失败: ${err.message}，降级到内存`);
        this.useRedis = false;
        this.startMemoryCleanup();
        tasks = Array.from(this.memoryStore.values());
      }
    } else {
      tasks = Array.from(this.memoryStore.values());
    }

    // 过滤
    if (options?.status) {
      tasks = tasks.filter((t) => t.status === options.status);
    }
    if (options?.type) {
      tasks = tasks.filter((t) => t.type === options.type);
    }

    // 按创建时间倒序
    tasks.sort((a, b) => b.created_at - a.created_at);

    return tasks.slice(0, limit);
  }

  async delete(id: string): Promise<void> {
    if (this.useRedis && this.redis) {
      try {
        await this.redis.del(`${KEY_PREFIX}${id}`);
        await this.redis.srem(INDEX_KEY, id);
        return;
      } catch (err: any) {
        logger.warn(`[TaskStore] Redis 删除失败: ${err.message}，降级到内存`);
        this.useRedis = false;
        this.startMemoryCleanup();
      }
    }

    this.memoryStore.delete(id);
  }

  getMode(): string {
    return this.useRedis ? "redis" : "memory";
  }
}

const taskStore = new TaskStore();
export default taskStore;
