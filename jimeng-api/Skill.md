---
name: jimeng-api
description: Generate images and videos using the Jimeng API (即梦AI). Use this skill when users request text-to-image, image-to-image, or video generation, or when they need to integrate Jimeng async task mode (`async: true`, `callback_url`, task polling via `/v1/tasks/:taskId`, cancellation, and webhook callbacks) into another project.
---

# Jimeng API

## Overview

This skill supports two integration paths on top of a locally deployed Jimeng API service (Docker):
- Synchronous local image generation via `scripts/generate_image.py` (downloads outputs to `/pic`).
- Asynchronous task integration via HTTP API (`async: true`) for image/composition/video generation with polling or webhook callback handling.

**API Endpoint**: `http://localhost:5100`

## When to Use This Skill

Use this skill when users request:
- "使用即梦生成图片 [描述]"
- "Generate an image using Jimeng: [description]"
- "Create artwork showing [scene/concept]"
- "Make an illustration of [subject]"
- "Generate a 4K image of [description]"
- "Transform this image to [style]" (image-to-image)
- "在其他项目接入即梦异步任务模式"
- "Add async task mode to my backend for Jimeng API"
- Any request involving Jimeng AI image generation or visual content creation

## Quick Start

### Prerequisites

**IMPORTANT**: The Jimeng API must be running locally via Docker before using this skill.

**Region-specific prefixes**:
   - 国内站: Direct sessionid (e.g., `your_session_id`)
   - 美国站: Add `us-` prefix (e.g., `us-your_session_id`)
   - 香港站: Add `hk-` prefix (e.g., `hk-your_session_id`)
   - 日本站: Add `jp-` prefix (e.g., `jp-your_session_id`)
   - 新加坡站: Add `sg-` prefix (e.g., `sg-your_session_id`)

**⚠️ nanobanana Model Resolution Rules**:
   - **US site (us-)**: Fixed at 1024×1024 with 2k resolution; ignores user-provided `ratio` and `resolution` parameters
   - **HK/JP/SG sites (hk-/jp-/sg-)**: Forced 1k resolution, but supports custom `ratio` parameters (e.g., 16:9, 4:3)
   - **Domestic site (CN)**: Does not support nanobanana model; use jimeng series instead

**Always ask the user for their Session ID before proceeding**, as the skill does not include a pre-configured credential.

Example prompt to user:
> "要使用即梦API生成图片,我需要您的Session ID。您可以从即梦网站(jimeng.jianying.com)的浏览器Cookie中获取 sessionid。
>
> 如果使用国际站,请在sessionid前添加对应前缀(us-/hk-/jp-/sg-)。
>
> 请提供您的 Session ID。"

## Parameter Usage Guidelines

⚠️⚠️ IMPORTANT PARAMETER DISCIPLINE

- ✅ **ONLY PASS PARAMETERS THE USER EXPLICITLY MENTIONS.**
- ❌ **DO NOT GUESS OR ADD UNSPECIFIED PARAMETERS.**
- ✅ **LET THE SCRIPT USE BUILT-IN DEFAULTS** when the user did not specify:
  - ratio: 1:1
  - resolution: 2k
  - model: jimeng-4.0
  - intelligent_ratio: false

Rationale: Tools may “helpfully” add options (e.g., `--ratio 16:9`) that the user didn’t request, overriding script defaults. This is prohibited. Pass only the parameters the user asked for; otherwise, rely on defaults.

### Basic Workflow (Select Sync or Async)

1. **Receive user request** for image generation
2. **Request Session ID** from the user if not already provided
3. **Clarify requirements**:
   - Text prompt (文生图) or input images (图生图)
   - Model selection (jimeng-4.0, jimeng-3.1, etc.)
   - Aspect ratio (1:1, 16:9, 4:3, etc.)
   - Resolution (1k, 2k, 4k)
   - Intelligent ratio (auto-detect based on prompt keywords)
4. **Select integration mode**:
   - **Sync script mode (image only)**: Use `scripts/generate_image.py`.
   - **Async API mode (image/composition/video)**: Submit with `"async": true`, persist `task_id`, then monitor task status via polling or webhook.
5. **Complete by mode**:
   - **Sync**: Report saved file paths only. **DO NOT READ/OPEN/ANALYZE GENERATED IMAGES.**
   - **Async**: Return `task_id` immediately, then continue until terminal status (`completed`/`failed`/`cancelled`) and report final result/error.

## Async Task Mode Integration (for Other Projects)

### API Contract

Use async mode on any generation endpoint by adding:

```json
{
  "async": true,
  "callback_url": "https://your-service.example/webhook"
}
```

- `async` (optional, default `false`): Enable async task mode.
- `callback_url` (optional): Receive completion/failure callbacks. Server retries 3 times with 5s/15s/30s delays.

Async submit response format:

```json
{
  "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "status": "pending"
}
```

### Task Endpoints

- Submit task:
  - `POST /v1/images/generations`
  - `POST /v1/images/edits`
  - `POST /v1/videos/generations`
- Query one task: `GET /v1/tasks/:taskId`
- List tasks: `GET /v1/tasks?status=...&type=...&limit=...`
- Cancel task: `POST /v1/tasks/:taskId/cancel` (only `pending` or `queued`)

### Task Status Lifecycle

Handle statuses as a state machine:

- Running states: `pending`, `queued`, `processing`
- Terminal success: `completed` (read `result`, same schema as sync response)
- Terminal failure: `failed` (read `error`)
- Terminal cancelled: `cancelled`

### Integration Checklist

1. Extend your generation request schema with `async?: boolean` and `callback_url?: string`.
2. Submit generation with `async: true`.
3. Persist `task_id` together with your business job ID.
4. Implement one completion strategy:
   - Poll `GET /v1/tasks/:taskId` every 3-5 seconds with timeout/backoff.
   - Or expose a webhook endpoint and pass `callback_url`.
5. On `completed`, consume `result` exactly like sync-mode output.
6. On `failed`, surface the `error` message to user/business logs.
7. Add cancellation support with `POST /v1/tasks/:taskId/cancel`.
8. Handle deployment persistence:
   - Prefer configuring `REDIS_URL` in production.
   - Without Redis, service falls back to memory mode and task records are lost on restart.

### Minimal Polling Example (TypeScript)

```ts
type TaskStatus = "pending" | "queued" | "processing" | "completed" | "failed" | "cancelled";

async function submitAsyncImage(baseUrl: string, sessionId: string, prompt: string) {
  const resp = await fetch(`${baseUrl}/v1/images/generations`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${sessionId}`,
    },
    body: JSON.stringify({
      model: "jimeng-4.5",
      prompt,
      async: true,
    }),
  });
  return await resp.json(); // { task_id, status: "pending" }
}

async function waitTaskDone(baseUrl: string, taskId: string, maxWaitMs = 10 * 60 * 1000) {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    const task = await fetch(`${baseUrl}/v1/tasks/${taskId}`).then((r) => r.json());
    const status = task.status as TaskStatus;
    if (status === "completed") return task.result;
    if (status === "failed" || status === "cancelled") throw new Error(task.error || status);
    await new Promise((r) => setTimeout(r, 5000));
  }
  throw new Error("Task polling timeout");
}
```

### Webhook Handling Notes

- Accept POST body schema identical to `GET /v1/tasks/:taskId`.
- Read headers:
  - `X-Webhook-Event`: `task.completed` or `task.failed`
  - `X-Task-Id`: task UUID
- Make handler idempotent (callbacks may retry).

## Image Generation Tasks

### Text-to-Image Generation

Generate images from text descriptions.

**Minimal default usage (no optional params):**
```bash
python scripts/generate_image.py text \
    "a cute cat" \
    --session-id "YOUR_SESSION_ID"
```

Only include optional parameters when the user explicitly requests them.

**With user-specified parameters (only when requested):**
```bash
python scripts/generate_image.py text \
    "futuristic city at sunset with flying cars" \
    --session-id "YOUR_SESSION_ID" \
    --model "jimeng-4.0" \
    --ratio "16:9" \
    --resolution "2k"
```

**Parameters:**
- `prompt` (required): Text description of the desired image
- `--session-id`: Jimeng session ID (required)
- `--model`: Model to use (default: `jimeng-4.0`)
  - Options: `jimeng-5.0`, `jimeng-4.6`, `jimeng-4.5`, `jimeng-4.1`, `jimeng-4.0`, `jimeng-3.1`, `jimeng-3.0`, `nanobanana` (international only)
- `--ratio`: Aspect ratio (default: `1:1`)
  - Options: `1:1`, `4:3`, `3:4`, `16:9`, `9:16`, `3:2`, `2:3`, `21:9`
- `--resolution`: Resolution level (default: `2k`)
  - Options: `1k`, `2k`, `4k`
- `--intelligent-ratio`: Enable smart ratio detection based on prompt keywords **⚠️ Only works for jimeng-4.0/jimeng-4.1/jimeng-4.5 models; other models will ignore this parameter**
- `--negative-prompt`: Negative prompt (elements to avoid)
- `--sample-strength`: Sampling strength (0.0-1.0)
- `--api-url`: Custom API URL (default: `http://localhost:5100`)
- `--output-dir`: Custom output directory (defaults to `project_root/pic`)

### Image-to-Image Composition

Transform or compose images based on text guidance.

**Example user request:**
> "把这张照片转换成油画风格,色彩鲜艳,笔触明显"

**Script usage:**
```bash
# Using local file
python scripts/generate_image.py image \
    "transform to oil painting style, vivid colors, visible brushstrokes" \
    --session-id "YOUR_SESSION_ID" \
    --images "/path/to/image.jpg"

# Using image URL
python scripts/generate_image.py image \
    "anime style, cute cat" \
    --session-id "YOUR_SESSION_ID" \
    --images "https://example.com/cat.jpg"

# Multiple images (up to 10)
python scripts/generate_image.py image \
    "merge these images into a cohesive scene" \
    --session-id "YOUR_SESSION_ID" \
    --images "image1.jpg" "image2.png" "image3.jpg"
```

**Parameters:**
- Same as text-to-image, plus:
- `--images`: One or more image paths or URLs (1-10 images)

**Supported formats**: JPG, PNG, WebP
**Size limit**: Recommended <10MB per image

### Intelligent Ratio Detection

**⚠️ IMPORTANT**: This feature only works with the `jimeng-4.0`, `jimeng-4.1`, and `jimeng-4.5` models. Other models (jimeng-3.0, nanobanana, etc.) will ignore the `--intelligent-ratio` flag.

Use `--intelligent-ratio` to automatically select the best aspect ratio based on prompt keywords.

**Example:**
```bash
python scripts/generate_image.py text \
    "奔跑的狮子,竖屏" \
    --session-id "YOUR_SESSION_ID" \
    --model "jimeng-4.0" \
    --intelligent-ratio
```

### Resolution Options

| Resolution | Ratio | Dimensions |
|------------|-------|------------|
| **1k** | 1:1 | 1024×1024 |
|  | 4:3 | 768×1024 |
|  | 3:4 | 1024×768 |
|  | 16:9 | 1024×576 |
|  | 9:16 | 576×1024 |
|  | 3:2 | 1024×682 |
|  | 2:3 | 682×1024 |
|  | 21:9 | 1195×512 |
| **2k** (default) | 1:1 | 2048×2048 |
|  | 16:9 | 2560×1440 |
|  | 4:3 | 2304×1728 |
| **4k** | 1:1 | 4096×4096 |
|  | 16:9 | 5120×2880 |
|  | 21:9 | 6048×2592 |

## Script Details

### Location
`scripts/generate_image.py`

### Key Features
- Automatic project root detection (looks for `.git`, `.claude`, etc.)
- Creates `/pic` folder if it doesn't exist
- Timestamps filenames to prevent overwrites (format: `jimeng_YYYYMMDD_HHMMSS_N.png`)
- Automatic WebP to PNG conversion for maximum compatibility
- Downloads all generated images from API response
- Supports both text-to-image and image-to-image modes
- Handles multipart/form-data for local file uploads
- Error handling for API calls and downloads
- Prints generation statistics

### Output Format
- Images are saved to: `{project_root}/pic/jimeng_{timestamp}_{index}.png`
- **All images are automatically converted to PNG format** (including WebP sources)
- Each API call generates several variations

### Requirements
The script requires:
```bash
pip install requests Pillow
```

**Note**: Pillow is required for WebP to PNG conversion. If not installed, WebP images will be saved as-is.

## Workflow Decision Tree

```
User requests image generation
    ↓
Is Jimeng API running at localhost:5100?
    ├─ No → Instruct user to start Docker service
    └─ Yes → Continue
    ↓
Do we have Session ID?
    ├─ No → Request Session ID from user → Store for session
    └─ Yes → Continue
    ↓
Sync script mode or Async API mode?
    ├─ Sync script mode (image only)
    │   ├─ Text-to-Image
    │   │   └─ Run: generate_image.py text "prompt" --session-id ID (add optional flags ONLY if user explicitly requests)
    │   └─ Image-to-Image
    │       └─ Run: generate_image.py image "prompt" --session-id ID --images PATH1 [PATH2...]
    └─ Async API mode (image/composition/video)
        ├─ Submit generation with "async": true
        ├─ Receive { task_id, status: "pending" }
        ├─ Poll GET /v1/tasks/:taskId or wait callback_url
        └─ Handle terminal status:
            ├─ completed → consume result
            ├─ failed → report error
            └─ cancelled → report cancellation
    ↓
If sync script mode:
    1. Calls Jimeng API (文生图 or 图生图)
    2. Receives image URLs
    3. Downloads all images to /pic folder
    4. Reports file paths
    ↓
    Inform user of results
        ├─ Sync success → Show file paths only
        ├─ Async success → Show final result payload
        └─ Failure → Report error, suggest troubleshooting
        ↓
    HARD STOP (sync image workflow only) — DO NOT READ/OPEN/ANALYZE IMAGES; DO NOT CALL `Read`/`view_image`; TASK COMPLETE
```

## Troubleshooting

### Common Issues

**"Session ID required"**
- Ensure the user has provided their sessionid from 即梦/Dreamina
- Verify correct region prefix (us-/hk-/jp-/sg- for international sites)

**"Invalid session or authentication failed"**
- Session ID may have expired
- Request user to refresh their browser and get a new sessionid
- Verify the sessionid is copied correctly (no extra spaces)

**"Error downloading image"**
- Check network connectivity
- Verify output directory is writable
- Image URLs may have expired (retry generation)

**"Model not supported"**
- `nanobanana` only works with international sites (us-/hk-/jp-/sg- prefix)
- `jimeng-3.1` only works with domestic sites

**"nanobanana resolution mismatch"**
- **US site (us- prefix)**: nanobanana model only supports 1024×1024 @ 2k resolution; all `ratio` and `resolution` parameters are ignored
- **HK/JP/SG sites (hk-/jp-/sg- prefix)**: nanobanana model forces 1k resolution, but allows custom ratios (e.g., 16:9, 4:3)
- If you need full control over resolution and ratio, use `jimeng-4.0` model instead

**"intelligent_ratio not working"**
- The `--intelligent-ratio` flag only works with `jimeng-4.0`, `jimeng-4.1`, and `jimeng-4.5` models
- Other models (jimeng-3.0, nanobanana, etc.) will ignore this parameter
- Solution: Use `jimeng-4.0`, `jimeng-4.1`, or `jimeng-4.5` if you need intelligent ratio detection

**"Task stuck in queued/processing"**
- Check service load and `TASK_MAX_CONCURRENT` setting
- Confirm worker/service process is healthy
- Query `GET /v1/tasks/:taskId` and inspect `queue_stats`

**"Task not found after service restart"**
- You are likely using in-memory fallback storage
- Configure `REDIS_URL` to persist task records across restarts

**"Webhook callback not received"**
- Verify `callback_url` is publicly reachable and returns 2xx
- Confirm receiver supports retries and idempotency
- Check callback logs with `X-Task-Id` for correlation

## Best Practices

1. **Request Session ID early** - Ask for it upfront if not already provided
2. **Parameter discipline: Only pass explicitly requested parameters**
3. **Clarify integration mode** - Decide sync script vs async API before implementation
4. **Use intelligent ratio when applicable** - Enable when prompt contains orientation hints
5. **Persist async task mapping** - Store `task_id` with your internal job ID for traceability
6. **Prefer webhook + polling fallback** - Webhook for push completion, polling for resilience
7. **Handle all variations** - Sync mode returns image URLs; async mode wraps final payload in `result`
8. **Inform about output location in sync script mode** - Always tell users where files are saved; **DO NOT read/open/analyze images; DO NOT call `Read`/`view_image`; STOP after saving**

## Example Interactions

**Example 1: Simple text-to-image**
```
User: "用即梦生成一张日落山景图"
Claude: "我来帮您使用即梦API生成图片。首先需要确认您的Session ID..."
[User provides sessionid]
Claude: [Executes generate_image.py text mode]
Claude: "✅ 成功生成4张图片!
- /project/pic/jimeng_20250116_153022_1.png
- /project/pic/jimeng_20250116_153022_2.png
- /project/pic/jimeng_20250116_153022_3.png
- /project/pic/jimeng_20250116_153022_4.png"
```

**Example 2: High-resolution with custom ratio**
```
User: "生成4K分辨率的21:9超宽屏壮丽山水风景"
Claude: [Executes with --resolution "4k" --ratio "21:9"]
Claude: "✅ 已生成4K超宽屏图片(6048×2592):
- 已保存到 /project/pic/jimeng_20250116_154530_1.png 等4张图片"
```

**Example 3: Image-to-image transformation**
```
User: "把这张猫咪照片转成动漫风格"
Claude: "请提供猫咪照片的路径或URL"
[User provides image path]
Claude: [Executes image mode with --images]
Claude: "✅ 图片风格转换完成,生成了4个动漫风格版本:
- /project/pic/jimeng_20250116_160015_1.png
- ..."
```

**Example 4: Intelligent ratio**
```
User: "生成一张竖屏手机壁纸,星空主题"
Claude: [Executes with --intelligent-ratio, auto-detects 9:16]
Claude: "✅ 已根据'竖屏'关键词自动选择9:16比例,生成星空壁纸4张"
```

**Example 5: Async integration for another backend**
```
User: "帮我在现有 Node.js 服务里接入即梦异步任务模式"
Claude:
1) 在生成请求里加 async/callback_url 字段
2) 提交任务后立即返回 task_id
3) 后台轮询 /v1/tasks/:taskId 或接收 callback_url 回调
4) completed 时使用 result，failed 时抛出 error
5) 加上 /v1/tasks/:taskId/cancel 取消逻辑
```

## API Response Format

Synchronous image generation response:

```json
{
    "created": 1763260188,
    "data": [
        {"url": "https://p3-dreamina-sign.byteimg.com/...image1.png"},
        {"url": "https://p26-dreamina-sign.byteimg.com/...image2.png"},
        {"url": "https://p26-dreamina-sign.byteimg.com/...image3.png"},
        {"url": "https://p3-dreamina-sign.byteimg.com/...image4.png"}
    ]
}
```

Async submit response:

```json
{
  "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "status": "pending"
}
```

Async task-completed response (`GET /v1/tasks/:taskId`):

```json
{
  "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "status": "completed",
  "type": "image",
  "result": {
    "created": 1763260188,
    "data": [
      {"url": "https://.../image1.png"},
      {"url": "https://.../image2.png"}
    ]
  }
}
```

## Security Notes

- ⚠️ **Never hardcode Session IDs** in scripts or skill files
- ⚠️ Session IDs are user-specific credentials; treat them as passwords
- ⚠️ Ensure the local API endpoint is not exposed publicly
- ⚠️ Image URLs from API responses may expire; download immediately
