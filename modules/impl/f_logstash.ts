import * as log4js from "log4js";

const logstashConfig = require("../../config/logstash.json");

log4js.configure({
    appenders: {
        logstash: {
            type: "@log4js-node/logstashudp",
            host: logstashConfig.localhost || "localhost",
            port: logstashConfig.port || 2334
        }
    },
    categories: {
        default: { appenders: ["logstash"], level: "info" }
    }
});

const logger = log4js.getLogger();

/**
 * 版本0,记录编号1
 * 游戏账号注册信息
 * @param line
 * @return {Promise<void>}
 */
export default async function f_logstash(line: string) {
    // 发送数据到logstash
    logger.info(line);
}

