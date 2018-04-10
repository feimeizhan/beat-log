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

let monitorConfig = require("./config/monitor.json");
let fileWorker: FileWorker;
let readWorker: ReadWorker;
let writeWorker: WriteWorker;

for (let item of monitorConfig) {
    if (!fs.existsSync(item.logDir)) {
        fs.mkdirSync(item.logDir);
    }

    if (!fs.existsSync(item.bakLogDir)) {
        fs.mkdirSync(item.bakLogDir);
    }

    fileWorker = new FileWorker(path.resolve(item.logDir), item.bakSize, item.bakSizeUnit, path.resolve(item.bakLogDir));
    readWorker = new ReadWorker(path.resolve(item.logDir), fileWorker, item.batchLineNum);
    writeWorker = new WriteWorker(item.gameName, item.batchLineNum);

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

process.on("uncaughtException", err => {
    console.error(err);
});

// 优雅关闭程序
process.on("SIGINT", () => {
    fileWorker.on(FileWorkerEvent.FILE_READY, () => {
        process.exit();
    });

    console.log("正在关闭程序...");

    // 在10秒之后强制关闭
    setTimeout((e) => {
        console.log("强制关闭程序...", e);

        process.exit(-1)
    }, 10000);
});
