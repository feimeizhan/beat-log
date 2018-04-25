/**
 * 两个数据库
 * 1.已读文件名DB
 * 2.无法处理内容DB
 */

import * as fs from "fs";
import {ReadWorker, ReadWorkerEvent} from "./modules/read_worker";
import {WriteWorker, WriteWorkerEvent} from "./modules/write_worker";
import {FileWorker, FileWorkerEvent} from "./modules/file_worker";
import * as path from "path";
import * as mkdirp from "mkdirp";
import {DBHelper} from "./db_helper";

let monitorConfig = require("./config/monitor.json");
let fileWorker: FileWorker;
let readWorker: ReadWorker;
let writeWorker: WriteWorker;

for (let item of monitorConfig) {
    if (!fs.existsSync(item.logDir)) {
        mkdirp.sync(item.logDir);
    }

    if (!fs.existsSync(item.bakLogDir)) {
        mkdirp.sync(item.bakLogDir);
    }

    fileWorker = new FileWorker({
        logDir: path.resolve(item.logDir),
        bakSize: item.bakSize,
        bakSizeUnit: item.bakSizeUnit,
        bakLogDir: path.resolve(item.bakLogDir),
        rdb: DBHelper.getReadFileDB(item.rdbDir),
        monitorInterval: item.monitorInterval
    });
    readWorker = new ReadWorker({
        logDir: path.resolve(item.logDir),
        fileWorker: fileWorker,
        batchLineNum: item.batchLineNum,
        uhdb: DBHelper.getUnHandleDB(item.uhdbDir)
    });
    writeWorker = new WriteWorker({
        gameName: item.gameName,
        uhdb: DBHelper.getUnHandleDB(item.uhdbDir)
    });

    readWorker.onStart();

    readWorker.on(ReadWorkerEvent.UHDB_ENOUGH, lineArray => {
        // 当前有足够的行数处理
        writeWorker.batchHandleLine(lineArray, true);
    });

    readWorker.on(ReadWorkerEvent.NORMAL_ENOUGH, lineArray => {
        writeWorker.batchHandleLine(lineArray, false);
    });

    writeWorker.on(WriteWorkerEvent.NORMAL_BATCH_FINISH, () => {
        // 已读完批量行
        readWorker.resumeReadLine();
    });

    writeWorker.on(WriteWorkerEvent.UH_BATCH_FINISH, () => {
        // 处理完之前未处理的日志
        // 扫描当前没有读过的日志文件
        readWorker.resumeUhdbKeyStream();
    });

    fileWorker.on(FileWorkerEvent.FILE_ALL_FINISH, (readFileCount, readFileTimeStamp) => {
        console.log(`当前已读取文件${readFileCount}个`);
        console.log(`耗时:${readFileTimeStamp}毫秒`);
    })
}

/**
 * 退出状态码
 */
enum EXIT_STATUS_CODE {
    /**
     * 空闲状态
     */
    IDLE = 100,

    /**
     * 下一个文件准备就绪状态
      * @type {number}
     */
    FILE_READY = 200,
    /**
     * 关闭超时
     * @type {number}
     */
    TIMEOUT = 300
}

// 优雅关闭程序
process.on("SIGINT", () => {
    if (readWorker.isIdle()) {
        process.exit(EXIT_STATUS_CODE.IDLE);
    }

    fileWorker.on(FileWorkerEvent.FILE_READY, () => {
        process.exit(EXIT_STATUS_CODE.FILE_READY);
    });

    console.log("正在关闭程序...");

    // 在10秒之后强制关闭
    setTimeout((e) => {
        console.log("强制关闭程序...", e);

        process.exit(EXIT_STATUS_CODE.TIMEOUT)
    }, 10000);
});
