import _ from "lodash";

import Request from "@/lib/request/Request.ts";
import taskStore from "@/lib/task-store.ts";
import taskQueue from "@/lib/task-queue.ts";
import { formatTaskResponse } from "@/lib/webhook.ts";
import type { TaskStatus, TaskType } from "@/lib/task-store.ts";

export default {
  prefix: "/v1/tasks",

  get: {
    // 列出任务
    "/": async (request: Request) => {
      const status = request.query.status as TaskStatus | undefined;
      const type = request.query.type as TaskType | undefined;
      const limit = request.query.limit ? parseInt(request.query.limit as string, 10) : 100;

      const tasks = await taskStore.list({ status, type, limit });
      const stats = taskQueue.getStats();

      return {
        tasks: tasks.map(formatTaskResponse),
        total: tasks.length,
        queue_stats: stats,
        storage_mode: taskStore.getMode(),
      };
    },

    // 查询单个任务
    "/:taskId": async (request: Request) => {
      const taskId = request.params.taskId;
      if (!taskId) {
        throw new Error("缺少 taskId 参数");
      }

      const task = await taskStore.get(taskId);
      if (!task) {
        throw new Error(`任务不存在: ${taskId}`);
      }

      const stats = taskQueue.getStats();
      return {
        ...formatTaskResponse(task),
        queue_stats: stats,
      };
    },
  },

  post: {
    // 取消排队中的任务
    "/:taskId/cancel": async (request: Request) => {
      const taskId = request.params.taskId;
      if (!taskId) {
        throw new Error("缺少 taskId 参数");
      }

      const task = await taskStore.get(taskId);
      if (!task) {
        throw new Error(`任务不存在: ${taskId}`);
      }

      // 只能取消 pending 或 queued 状态的任务
      if (task.status !== "pending" && task.status !== "queued") {
        throw new Error(`任务 ${taskId} 当前状态为 ${task.status}，无法取消。仅 pending/queued 状态可取消。`);
      }

      // 从队列中移除
      taskQueue.cancelQueued(taskId);

      await taskStore.update(taskId, {
        status: "cancelled",
        completed_at: Math.floor(Date.now() / 1000),
      });

      return {
        task_id: taskId,
        status: "cancelled",
        message: "任务已取消",
      };
    },
  },
};
