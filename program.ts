/**
 * 两个数据库
 * 1.已读文件名DB
 * 2.无法处理内容DB
 */

import * as fs from "fs";
import {ReadWorker, ReadWorkerEvent} from "./modules/read_worker";
import {WriteWorker, WriteWorkerEvent} from "./modules/write_worker";
import {FileWorker} from "./modules/file_worker";
import * as path from "path";

let monitorConfig = require("./config/monitor.json");

// 让数据库充分初始化
setTimeout(() => {
    for (let item of monitorConfig) {
        if (!fs.existsSync(item.logDir)) {
            fs.mkdirSync(item.logDir);
        }

        if (!fs.existsSync(item.bakLogDir)) {
            fs.mkdirSync(item.bakLogDir);
        }

        let fileWorker: FileWorker = new FileWorker(path.resolve(item.logDir), item.batchFileNum, item.bakSize, item.bakSizeUnit, path.resolve(item.bakLogDir));
        let readWorker: ReadWorker = new ReadWorker(path.resolve(item.logDir), fileWorker, item.batchLineNum);
        let writeWorker: WriteWorker = new WriteWorker(item.gameName, item.batchLineNum);

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
    }
}, 5000);

process.on("uncaughtException", err => {
    console.error(err);
});

// 优雅关闭程序
process.on("SIGINT", () => {
    const cleanUp = () => {
        // Clean up other resources like DB connections
    };

    console.log("正在关闭程序...");

    // server.close(() => {
    //     console.log("Server closed !!! ")
    //
    //     cleanUp();
    //     process.exit()
    // });

    // Force close server after 5secs
    setTimeout((e) => {
        console.log("强制关闭程序...", e)

        cleanUp();
        process.exit(1)
    }, 5000);
});
