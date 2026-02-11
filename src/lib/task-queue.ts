import logger from "./logger.ts";
import taskStore from "./task-store.ts";

interface QueueItem {
  taskId: string;
  run: () => Promise<void>;
}

class TaskQueue {
  private maxConcurrent: number;
  private running = 0;
  private queue: QueueItem[] = [];

  constructor() {
    this.maxConcurrent = parseInt(process.env.TASK_MAX_CONCURRENT || "50", 10);
    logger.info(`[TaskQueue] 最大并发任务数: ${this.maxConcurrent}`);
  }

  async enqueue(taskId: string, fn: () => Promise<void>): Promise<void> {
    if (this.running < this.maxConcurrent) {
      this.running++;
      logger.info(`[TaskQueue] 任务 ${taskId} 直接执行 (运行中: ${this.running}/${this.maxConcurrent}, 排队: ${this.queue.length})`);
      this.execute(taskId, fn);
    } else {
      this.queue.push({ taskId, run: fn });
      await taskStore.update(taskId, { status: "queued", progress: `排队中 (位置: ${this.queue.length})` });
      logger.info(`[TaskQueue] 任务 ${taskId} 进入排队 (运行中: ${this.running}/${this.maxConcurrent}, 排队: ${this.queue.length})`);
    }
  }

  private async execute(taskId: string, fn: () => Promise<void>): Promise<void> {
    try {
      await fn();
    } catch (err: any) {
      logger.error(`[TaskQueue] 任务 ${taskId} 执行异常: ${err.message}`);
    } finally {
      this.running--;
      this.dequeue();
    }
  }

  private dequeue(): void {
    if (this.queue.length === 0) return;
    if (this.running >= this.maxConcurrent) return;

    const next = this.queue.shift();
    if (!next) return;

    this.running++;
    logger.info(`[TaskQueue] 任务 ${next.taskId} 从队列取出执行 (运行中: ${this.running}/${this.maxConcurrent}, 排队: ${this.queue.length})`);
    this.execute(next.taskId, next.run);
  }

  /**
   * 取消排队中的任务（仅能取消尚未执行的排队任务）
   * 返回 true 表示成功取消，false 表示任务不在排队中
   */
  cancelQueued(taskId: string): boolean {
    const index = this.queue.findIndex((item) => item.taskId === taskId);
    if (index === -1) return false;
    this.queue.splice(index, 1);
    logger.info(`[TaskQueue] 任务 ${taskId} 已从排队中取消 (排队: ${this.queue.length})`);
    return true;
  }

  getStats(): { running: number; queued: number; maxConcurrent: number } {
    return {
      running: this.running,
      queued: this.queue.length,
      maxConcurrent: this.maxConcurrent,
    };
  }
}

const taskQueue = new TaskQueue();
export default taskQueue;
