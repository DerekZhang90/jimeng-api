import _ from 'lodash';

import Request from '@/lib/request/Request.ts';
import Response from '@/lib/response/Response.ts';
import { tokenSplit, parseProxyFromToken } from '@/api/controllers/core.ts';
import { generateVideo, DEFAULT_MODEL } from '@/api/controllers/videos.ts';
import util from '@/lib/util.ts';
import taskStore from '@/lib/task-store.ts';
import taskQueue from '@/lib/task-queue.ts';
import { sendWebhook } from '@/lib/webhook.ts';
import logger from '@/lib/logger.ts';

const MAX_STANDARD_INPUT_IMAGES = 2;
const MAX_OMNI_MATERIALS = 9;
const OMNI_IMAGE_FIELD_PATTERN = /^image_file_(\d+)$/;
const OMNI_VIDEO_FIELD_PATTERN = /^video_file(?:_(\d+))?$/;

function collectUploadedFileEntries(files: any): Array<{ fieldName: string; file: any }> {
    if (!files || !_.isObject(files)) return [];
    const entries: Array<{ fieldName: string; file: any }> = [];

    for (const [fieldName, fieldValue] of Object.entries(files)) {
        const fileList = Array.isArray(fieldValue) ? fieldValue : [fieldValue];
        for (const file of fileList) {
            if (file) entries.push({ fieldName, file });
        }
    }

    return entries;
}

function isOmniMaterialField(fieldName: string): boolean {
    const imageMatch = fieldName.match(OMNI_IMAGE_FIELD_PATTERN);
    if (imageMatch) {
        const idx = Number(imageMatch[1]);
        return Number.isInteger(idx) && idx >= 1 && idx <= MAX_OMNI_MATERIALS;
    }

    const videoMatch = fieldName.match(OMNI_VIDEO_FIELD_PATTERN);
    if (!videoMatch) return false;
    if (!videoMatch[1]) return true; // 兼容旧字段名 video_file
    const idx = Number(videoMatch[1]);
    return Number.isInteger(idx) && idx >= 1 && idx <= MAX_OMNI_MATERIALS;
}

function collectOmniBodyMaterialUrls(body: any): Record<string, string> {
    const materialUrls: Record<string, string> = {};
    if (!body || !_.isObject(body)) return materialUrls;

    for (const [key, value] of Object.entries(body)) {
        if (typeof value === 'string' && value.startsWith('http') && isOmniMaterialField(key)) {
            materialUrls[key] = value;
        }
    }

    return materialUrls;
}

export default {

    prefix: '/v1/videos',

    post: {

        '/generations': async (request: Request) => {
            const contentType = request.headers['content-type'] || '';
            const isMultiPart = contentType.startsWith('multipart/form-data');

            request
                .validate('body.model', v => _.isUndefined(v) || _.isString(v))
                .validate('body.prompt', _.isString)
                .validate('body.ratio', v => _.isUndefined(v) || _.isString(v))
                .validate('body.resolution', v => _.isUndefined(v) || _.isString(v))
                .validate('body.duration', v => {
                    if (_.isUndefined(v)) return true;
                    // 支持的时长范围: 4~15 (seedance 2.0 支持任意整数秒)
                    let num: number;
                    if (isMultiPart && typeof v === 'string') {
                        num = parseInt(v);
                    } else if (_.isFinite(v)) {
                        num = v as number;
                    } else {
                        return false;
                    }
                    return Number.isInteger(num) && num >= 4 && num <= 15;
                })
                .validate('body.file_paths', v => _.isUndefined(v) || _.isArray(v))
                .validate('body.filePaths', v => _.isUndefined(v) || _.isArray(v))
                .validate('body.functionMode', v => _.isUndefined(v) || (_.isString(v) && ['first_last_frames', 'omni_reference'].includes(v)))
                .validate('body.response_format', v => _.isUndefined(v) || _.isString(v))
                .validate('body.async', v => _.isUndefined(v) || _.isBoolean(v) || v === 'true' || v === 'false')
                .validate('body.callback_url', v => _.isUndefined(v) || (_.isString(v) && v.startsWith('http')))
                // Optional anti-bot / fingerprint parameters (CN site may require these)
                .validate('body.msToken', v => _.isUndefined(v) || _.isString(v))
                .validate('body.ms_token', v => _.isUndefined(v) || _.isString(v))
                .validate('body.a_bogus', v => _.isUndefined(v) || _.isString(v))
                .validate('body.aBogus', v => _.isUndefined(v) || _.isString(v))
                .validate('body.webId', v => _.isUndefined(v) || _.isString(v) || _.isFinite(v))
                .validate('body.web_id', v => _.isUndefined(v) || _.isString(v) || _.isFinite(v))
                .validate('body.os', v => _.isUndefined(v) || _.isString(v))
                .validate('body.userAgent', v => _.isUndefined(v) || _.isString(v))
                .validate('body.user_agent', v => _.isUndefined(v) || _.isString(v))
                .validate('body.secChUaPlatform', v => _.isUndefined(v) || _.isString(v))
                .validate('body.sec_ch_ua_platform', v => _.isUndefined(v) || _.isString(v))
                .validate('body.referer', v => _.isUndefined(v) || _.isString(v))
                .validate('headers.authorization', _.isString);

            const functionMode = request.body.functionMode || 'first_last_frames';
            const isOmniMode = functionMode === 'omni_reference';

            const {
                model = DEFAULT_MODEL,
                prompt,
                ratio = "1:1",
                resolution = "720p",
                duration = 5,
                file_paths = [],
                filePaths = [],
                response_format = "url",
                // optional anti-bot / fingerprint fields
                msToken,
                ms_token,
                a_bogus,
                aBogus,
                webId,
                web_id,
                os,
                userAgent,
                user_agent,
                secChUaPlatform,
                sec_ch_ua_platform,
                referer,
            } = request.body;

            const finalMsToken = msToken || ms_token;
            const finalABogus = a_bogus || aBogus;
            const finalWebId = webId || web_id;
            const finalUserAgent = userAgent || user_agent;
            const finalSecChUaPlatform = secChUaPlatform || sec_ch_ua_platform;

            // 兼容两种参数名格式：file_paths 和 filePaths
            const finalFilePaths = filePaths.length > 0 ? filePaths : file_paths;
            if (!isOmniMode && finalFilePaths.length > MAX_STANDARD_INPUT_IMAGES) {
                throw new Error('最多支持2张输入图片');
            }
            if (isOmniMode && finalFilePaths.length > MAX_OMNI_MATERIALS) {
                throw new Error('全能模式通过 file_paths/filePaths 最多支持9个素材URL');
            }

            const uploadedFileEntries = collectUploadedFileEntries(request.files);
            const omniUploadedEntries = uploadedFileEntries.filter(entry => isOmniMaterialField(entry.fieldName));
            const uploadedMaterialCount = isOmniMode ? omniUploadedEntries.length : uploadedFileEntries.length;
            const maxUploadedFiles = isOmniMode ? MAX_OMNI_MATERIALS : MAX_STANDARD_INPUT_IMAGES;
            if (uploadedMaterialCount > maxUploadedFiles) {
                throw new Error(isOmniMode
                    ? '全能模式最多上传9个素材文件'
                    : '最多只能上传2个图片文件');
            }
            if (isOmniMode && uploadedFileEntries.length > omniUploadedEntries.length) {
                throw new Error('全能模式仅支持 image_file_1~image_file_9、video_file、video_file_1~video_file_9 字段');
            }

            const materialUrls = isOmniMode ? collectOmniBodyMaterialUrls(request.body) : {};
            const hasFilePaths = finalFilePaths.length > 0;
            const hasMaterialUrls = Object.keys(materialUrls).length > 0;
            if (isOmniMode && uploadedMaterialCount === 0 && !hasFilePaths && !hasMaterialUrls) {
                throw new Error('全能模式(omni_reference)至少需要上传1个素材文件(图片或视频)或提供素材URL');
            }

            // refresh_token切分
            const tokens = tokenSplit(request.headers.authorization);
            // 随机挑选一个refresh_token
            const token = _.sample(tokens);

            // 如果是 multipart/form-data，需要将字符串转换为数字
            const finalDuration = isMultiPart && typeof duration === 'string'
                ? parseInt(duration)
                : duration;

            // 处理 multipart 中的 async 参数（字符串转布尔）
            const isAsync = isMultiPart && typeof request.body.async === 'string'
                ? request.body.async === 'true'
                : request.body.async === true;

            // ====== 异步模式 ======
            if (isAsync) {
                const task = await taskStore.create({
                    type: "video",
                    status: "pending",
                    callback_url: request.body.callback_url,
                    model,
                    prompt,
                });

                taskQueue.enqueue(task.id, async () => {
                    try {
                        await taskStore.update(task.id, { status: "processing", progress: "生成中" });
                        const generatedVideo = await generateVideo(
                            model,
                            prompt,
                            {
                                ratio,
                                resolution,
                                duration: finalDuration,
                                filePaths: finalFilePaths,
                                files: request.files,
                                materialUrls,
                                functionMode,
                                msToken: finalMsToken,
                                a_bogus: finalABogus,
                                webId: finalWebId,
                                os,
                                userAgent: finalUserAgent,
                                secChUaPlatform: finalSecChUaPlatform,
                                referer,
                            },
                            token
                        );

                        let resultData: any;
                        if (response_format === "b64_json") {
                            const { proxyUrl } = parseProxyFromToken(token);
                            const videoBase64 = await util.fetchFileBASE64(generatedVideo.url, { proxyUrl: proxyUrl || undefined });
                            resultData = {
                                created: util.unixTimestamp(),
                                data: [{
                                    b64_json: videoBase64,
                                    revised_prompt: prompt,
                                    history_id: generatedVideo.history_id,
                                    item_id: generatedVideo.item_id,
                                    preview_url: generatedVideo.preview_url,
                                    hq_url: generatedVideo.hq_url,
                                }]
                            };
                        } else {
                            resultData = {
                                created: util.unixTimestamp(),
                                data: [{
                                    url: generatedVideo.url,
                                    revised_prompt: prompt,
                                    history_id: generatedVideo.history_id,
                                    item_id: generatedVideo.item_id,
                                    preview_url: generatedVideo.preview_url,
                                    hq_url: generatedVideo.hq_url,
                                }]
                            };
                        }

                        await taskStore.update(task.id, {
                            status: "completed",
                            result: resultData,
                            completed_at: Math.floor(Date.now() / 1000),
                        });
                    } catch (err: any) {
                        logger.error(`[Async] 视频生成任务 ${task.id} 失败: ${err.message}`);
                        await taskStore.update(task.id, {
                            status: "failed",
                            error: err.message,
                            completed_at: Math.floor(Date.now() / 1000),
                        });
                    }
                    // Webhook 回调
                    const finalTask = await taskStore.get(task.id);
                    if (finalTask?.callback_url) {
                        await sendWebhook(finalTask.callback_url, finalTask);
                    }
                });

                return { task_id: task.id, status: "pending" };
            }

            // ====== 同步模式（原有逻辑不变） ======
            const generatedVideo = await generateVideo(
                model,
                prompt,
                {
                    ratio,
                    resolution,
                    duration: finalDuration,
                    filePaths: finalFilePaths,
                    files: request.files, // 传递上传的文件
                    materialUrls,         // 传递 body 中的 URL 素材字段
                    functionMode,
                    msToken: finalMsToken,
                    a_bogus: finalABogus,
                    webId: finalWebId,
                    os,
                    userAgent: finalUserAgent,
                    secChUaPlatform: finalSecChUaPlatform,
                    referer,
                },
                token
            );

            // 根据response_format返回不同格式的结果
            if (response_format === "b64_json") {
                // 获取视频内容并转换为BASE64
                const { proxyUrl } = parseProxyFromToken(token);
                const videoBase64 = await util.fetchFileBASE64(generatedVideo.url, { proxyUrl: proxyUrl || undefined });
                return {
                    created: util.unixTimestamp(),
                    data: [{
                        b64_json: videoBase64,
                        revised_prompt: prompt,
                        history_id: generatedVideo.history_id,
                        item_id: generatedVideo.item_id,
                        preview_url: generatedVideo.preview_url,
                        hq_url: generatedVideo.hq_url,
                    }]
                };
            } else {
                // 默认返回URL
                return {
                    created: util.unixTimestamp(),
                    data: [{
                        url: generatedVideo.url,
                        revised_prompt: prompt,
                        history_id: generatedVideo.history_id,
                        item_id: generatedVideo.item_id,
                        preview_url: generatedVideo.preview_url,
                        hq_url: generatedVideo.hq_url,
                    }]
                };
            }
        }

    }

}
