import axios from "axios";
import logger from "./logger.ts";
import type { Task } from "./task-store.ts";

const WEBHOOK_TIMEOUT = 10000; // 10 秒超时
const RETRY_DELAYS = [5000, 15000, 30000]; // 指数退避: 5s, 15s, 30s

function formatTaskResponse(task: Task): any {
  const response: any = {
    task_id: task.id,
    type: task.type,
    status: task.status,
    model: task.model,
    prompt: task.prompt,
    created_at: task.created_at,
    updated_at: task.updated_at,
  };

  if (task.completed_at) {
    response.completed_at = task.completed_at;
  }

  if (task.status === "completed" && task.result) {
    response.result = task.result;
  }

  if (task.status === "failed" && task.error) {
    response.error = task.error;
  }

  return response;
}

async function sendWebhook(callbackUrl: string, task: Task): Promise<void> {
  const payload = formatTaskResponse(task);

  for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
    try {
      const response = await axios.post(callbackUrl, payload, {
        timeout: WEBHOOK_TIMEOUT,
        headers: {
          "Content-Type": "application/json",
          "X-Webhook-Event": `task.${task.status}`,
          "X-Task-Id": task.id,
        },
        // 2xx 和 3xx 都算成功
        validateStatus: (status) => status >= 200 && status < 400,
      });

      logger.info(`[Webhook] 任务 ${task.id} 回调成功: ${callbackUrl} (status: ${response.status})`);
      return;
    } catch (err: any) {
      const isLastAttempt = attempt >= RETRY_DELAYS.length;
      if (isLastAttempt) {
        logger.error(
          `[Webhook] 任务 ${task.id} 回调最终失败 (${attempt + 1} 次尝试): ${callbackUrl} - ${err.message}`
        );
        return; // 不抛出异常，不影响任务状态
      }

      const delay = RETRY_DELAYS[attempt];
      logger.warn(
        `[Webhook] 任务 ${task.id} 回调失败 (第 ${attempt + 1} 次): ${err.message}，${delay / 1000}s 后重试`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

export { sendWebhook, formatTaskResponse };
