import crypto from "crypto";
import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";
import { SocksProxyAgent } from "socks-proxy-agent";
import { RegionInfo, parseProxyFromToken, request } from "@/api/controllers/core.ts";
import { RegionUtils } from "@/lib/region-utils.ts";
import { createSignature } from "@/lib/aws-signature.ts";
import logger from "@/lib/logger.ts";
import util from "@/lib/util.ts";

/**
 * 统一的图片上传模块
 * 整合了images.ts和videos.ts中重复的上传逻辑
 */

const DEFAULT_NETWORK_TIMEOUT_MS = 45000;
const DEFAULT_RETRY_DELAY_MS = 5000;
const DEFAULT_MAX_RETRIES = 3;

const RETRYABLE_ERROR_CODES = new Set([
  "ECONNABORTED",
  "ETIMEDOUT",
  "ECONNRESET",
  "ENOTFOUND",
  "ECONNREFUSED",
  "EAI_AGAIN",
  "EPIPE",
  "ENETUNREACH",
  "EHOSTUNREACH",
]);

function isRetryableNetworkError(error: any): boolean {
  const message = String(error?.message || "");
  return (
    RETRYABLE_ERROR_CODES.has(String(error?.code || "")) ||
    message.includes("timeout") ||
    message.includes("network") ||
    message.includes("ECONNRESET") ||
    message.includes("socket hang up") ||
    message.includes("wrong version number") ||
    message.includes("Client network socket disconnected") ||
    message.includes("Proxy connection")
  );
}

function maskProxyUrl(proxyUrl: string): string {
  return proxyUrl.replace(/\/\/([^@/]+)@/i, "//***@");
}

async function sleep(ms: number) {
  await new Promise(resolve => setTimeout(resolve, ms));
}

async function axiosWithRetry<T>(
  operation: () => Promise<T>,
  context: string,
  options?: { maxRetries?: number; retryDelayMs?: number }
): Promise<T> {
  const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
  const retryDelayMs = options?.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;

  let retries = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await operation();
    } catch (error: any) {
      if (!isRetryableNetworkError(error) || retries >= maxRetries) throw error;
      retries++;
      logger.warn(`${context} 网络错误: ${error.message}，${retryDelayMs / 1000}s 后重试 (${retries}/${maxRetries})`);
      await sleep(retryDelayMs);
    }
  }
}

/**
 * 上传图片Buffer到ImageX
 * @param imageBuffer 图片数据
 * @param refreshToken 刷新令牌
 * @param regionInfo 区域信息
 * @returns 图片URI
 */
export async function uploadImageBuffer(
  imageBuffer: ArrayBuffer | Buffer,
  refreshToken: string,
  regionInfo: RegionInfo
  ): Promise<string> {
  try {
    logger.info(`开始上传图片Buffer... (isInternational: ${regionInfo.isInternational})`);

    const { proxyUrl } = parseProxyFromToken(refreshToken);
    const proxyAgent = proxyUrl
      ? (proxyUrl.toLowerCase().startsWith("socks")
        ? new SocksProxyAgent(proxyUrl)
        : new HttpsProxyAgent(proxyUrl))
      : undefined;
    if (proxyUrl) {
      logger.info(`ImageX 上传使用代理: ${maskProxyUrl(proxyUrl)}`);
    }

    // 第一步：获取上传令牌
    const tokenResult = await request("post", "/mweb/v1/get_upload_token", refreshToken, {
      data: {
        scene: 2, // AIGC 图片上传场景
      },
    });

    const { access_key_id, secret_access_key, session_token } = tokenResult;
    const service_id = regionInfo.isInternational ? tokenResult.space_name : tokenResult.service_id;

    if (!access_key_id || !secret_access_key || !session_token) {
      throw new Error("获取上传令牌失败");
    }

    const actualServiceId = RegionUtils.getServiceId(regionInfo, service_id);
    logger.info(`获取上传令牌成功: service_id=${actualServiceId}`);

    // 准备文件信息
    const fileSize = imageBuffer.byteLength;
    const crc32 = util.calculateCRC32(imageBuffer);
    logger.info(`图片Buffer: 大小=${fileSize}字节, CRC32=${crc32}`);

    // 第二步：申请图片上传权限
    const now = new Date();
    const timestamp = now.toISOString().replace(/[:\-]/g, '').replace(/\.\d{3}Z$/, 'Z');
    const randomStr = Math.random().toString(36).substring(2, 12);

    const applyUrlHost = RegionUtils.getImageXUrl(regionInfo);
    const applyUrl = `${applyUrlHost}/?Action=ApplyImageUpload&Version=2018-08-01&ServiceId=${actualServiceId}&FileSize=${fileSize}&s=${randomStr}${regionInfo.isInternational ? '&device_platform=web' : ''}`;

    const awsRegion = RegionUtils.getAWSRegion(regionInfo);
    const origin = RegionUtils.getOrigin(regionInfo);

    const requestHeaders = {
      'x-amz-date': timestamp,
      'x-amz-security-token': session_token
    };

    const authorization = createSignature('GET', applyUrl, requestHeaders, access_key_id, secret_access_key, session_token, '', awsRegion);

    logger.info(`申请上传权限: ${applyUrl}`);

    let applyResponse;
    try {
      applyResponse = await axiosWithRetry(
        () => axios({
          method: 'GET',
          url: applyUrl,
          headers: {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'authorization': authorization,
            'origin': origin,
            'referer': `${origin}/ai-tool/generate`,
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'x-amz-date': timestamp,
            'x-amz-security-token': session_token,
          },
          timeout: DEFAULT_NETWORK_TIMEOUT_MS,
          ...(proxyAgent ? { httpAgent: proxyAgent, httpsAgent: proxyAgent, proxy: false } : {}),
          validateStatus: () => true,
        }),
        `ApplyImageUpload (${applyUrlHost})`
      );
    } catch (fetchError: any) {
      logger.error(`Fetch请求失败，目标URL: ${applyUrl}`);
      logger.error(`错误详情: ${fetchError.message}`);
      throw new Error(`网络请求失败 (${applyUrlHost}): ${fetchError.message}. 请检查: 1) 网络连接是否正常 2) 是否需要配置代理 3) DNS是否能解析该域名`);
    }

    if (applyResponse.status < 200 || applyResponse.status >= 300) {
      const errorText = typeof applyResponse.data === 'string' ? applyResponse.data : JSON.stringify(applyResponse.data);
      throw new Error(`申请上传权限失败: ${applyResponse.status} - ${errorText}`);
    }

    const applyResult = applyResponse.data;

    if (applyResult?.ResponseMetadata?.Error) {
      throw new Error(`申请上传权限失败: ${JSON.stringify(applyResult.ResponseMetadata.Error)}`);
    }

    logger.info(`申请上传权限成功`);

    // 解析上传信息
    const uploadAddress = applyResult?.Result?.UploadAddress;
    if (!uploadAddress || !uploadAddress.StoreInfos || !uploadAddress.UploadHosts) {
      throw new Error(`获取上传地址失败: ${JSON.stringify(applyResult)}`);
    }

    const storeInfo = uploadAddress.StoreInfos[0];
    const uploadHost = uploadAddress.UploadHosts[0];
    const auth = storeInfo.Auth;
    const uploadUrl = `https://${uploadHost}/upload/v1/${storeInfo.StoreUri}`;

    logger.info(`准备上传图片: uploadUrl=${uploadUrl}`);

    // 第三步：上传图片文件
    let uploadResponse;
    try {
      uploadResponse = await axiosWithRetry(
        () => axios({
          method: 'POST',
          url: uploadUrl,
          headers: {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Authorization': auth,
            'Connection': 'keep-alive',
            'Content-CRC32': crc32,
            'Content-Disposition': 'attachment; filename="undefined"',
            'Content-Type': 'application/octet-stream',
            'Origin': origin,
            'Referer': RegionUtils.getRefererPath(regionInfo),
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
          },
          data: imageBuffer,
          timeout: DEFAULT_NETWORK_TIMEOUT_MS,
          ...(proxyAgent ? { httpAgent: proxyAgent, httpsAgent: proxyAgent, proxy: false } : {}),
          // 图片体积可能较大，放宽 axios 默认限制
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
          validateStatus: () => true,
        }),
        `UploadImage (${uploadHost})`
      );
    } catch (fetchError: any) {
      logger.error(`图片文件上传fetch请求失败，目标URL: ${uploadUrl}`);
      logger.error(`错误详情: ${fetchError.message}`);
      throw new Error(`图片上传网络请求失败 (${uploadHost}): ${fetchError.message}. 请检查网络连接`);
    }

    if (uploadResponse.status < 200 || uploadResponse.status >= 300) {
      const errorText = typeof uploadResponse.data === 'string' ? uploadResponse.data : JSON.stringify(uploadResponse.data);
      throw new Error(`图片上传失败: ${uploadResponse.status} - ${errorText}`);
    }

    logger.info(`图片文件上传成功`);

    // 第四步：提交上传
    const commitUrl = `${applyUrlHost}/?Action=CommitImageUpload&Version=2018-08-01&ServiceId=${actualServiceId}`;
    const commitTimestamp = new Date().toISOString().replace(/[:\-]/g, '').replace(/\.\d{3}Z$/, 'Z');
    const commitPayload = JSON.stringify({
      SessionKey: uploadAddress.SessionKey
    });

    const payloadHash = crypto.createHash('sha256').update(commitPayload, 'utf8').digest('hex');

    const commitRequestHeaders = {
      'x-amz-date': commitTimestamp,
      'x-amz-security-token': session_token,
      'x-amz-content-sha256': payloadHash
    };

    const commitAuthorization = createSignature('POST', commitUrl, commitRequestHeaders, access_key_id, secret_access_key, session_token, commitPayload, awsRegion);

    let commitResponse;
    try {
      commitResponse = await axiosWithRetry(
        () => axios({
          method: 'POST',
          url: commitUrl,
          headers: {
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'authorization': commitAuthorization,
            'content-type': 'application/json',
            'origin': origin,
            'referer': RegionUtils.getRefererPath(regionInfo),
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'x-amz-date': commitTimestamp,
            'x-amz-security-token': session_token,
            'x-amz-content-sha256': payloadHash,
          },
          data: commitPayload,
          timeout: DEFAULT_NETWORK_TIMEOUT_MS,
          ...(proxyAgent ? { httpAgent: proxyAgent, httpsAgent: proxyAgent, proxy: false } : {}),
          validateStatus: () => true,
        }),
        `CommitImageUpload (${applyUrlHost})`
      );
    } catch (fetchError: any) {
      logger.error(`提交上传fetch请求失败，目标URL: ${commitUrl}`);
      logger.error(`错误详情: ${fetchError.message}`);
      throw new Error(`提交上传网络请求失败 (${applyUrlHost}): ${fetchError.message}. 请检查网络连接`);
    }

    if (commitResponse.status < 200 || commitResponse.status >= 300) {
      const errorText = typeof commitResponse.data === 'string' ? commitResponse.data : JSON.stringify(commitResponse.data);
      throw new Error(`提交上传失败: ${commitResponse.status} - ${errorText}`);
    }

    const commitResult = commitResponse.data;

    if (commitResult?.ResponseMetadata?.Error) {
      throw new Error(`提交上传失败: ${JSON.stringify(commitResult.ResponseMetadata.Error)}`);
    }

    if (!commitResult?.Result?.Results || commitResult.Result.Results.length === 0) {
      throw new Error(`提交上传响应缺少结果: ${JSON.stringify(commitResult)}`);
    }

    const uploadResult = commitResult.Result.Results[0];
    if (uploadResult.UriStatus !== 2000) {
      throw new Error(`图片上传状态异常: UriStatus=${uploadResult.UriStatus}`);
    }

    const fullImageUri = uploadResult.Uri;
    logger.info(`图片上传完成: ${fullImageUri}`);

    return fullImageUri;
  } catch (error: any) {
    logger.error(`图片Buffer上传失败: ${error.message}`);
    throw error;
  }
}

/**
 * 从URL下载并上传图片
 * @param imageUrl 图片URL
 * @param refreshToken 刷新令牌
 * @param regionInfo 区域信息
 * @returns 图片URI
 */
export async function uploadImageFromUrl(
  imageUrl: string,
  refreshToken: string,
  regionInfo: RegionInfo
): Promise<string> {
  try {
    logger.info(`开始从URL下载并上传图片: ${imageUrl}`);

    const imageResponse = await axios.get(imageUrl, {
      responseType: 'arraybuffer',
    });
    if (imageResponse.status < 200 || imageResponse.status >= 300) {
      throw new Error(`下载图片失败: ${imageResponse.status}`);
    }

    const imageBuffer = imageResponse.data;
    return await uploadImageBuffer(imageBuffer, refreshToken, regionInfo);
  } catch (error: any) {
    logger.error(`从URL上传图片失败: ${error.message}`);
    throw error;
  }
}
