import * as axios from "axios";
import * as os from "os";

const sender = axios.default.create({
    baseURL: `http://${process.env.LOG_HOST || "localhost"}:${process.env.LOG_PORT || 2334}`,
    timeout: +process.env.LOG_TIMEOUT || 5000,
    headers: {"Content-Type": "application/json"}, // 使用_bulk接口需要使用application/x-ndjson
    withCredentials: true
});

/**
 * 版本0,记录编号1
 * 游戏账号注册信息
 * @param line
 * @return {Promise<void>}
 */
export default async function f_logstash(line: string) {
    // 发送数据到logstash
    await sender.post('',{
        '@version': 1,
        '@timestamp': (new Date().toISOString()),
        'host': os.hostname(),
        'message': line
    });
}

