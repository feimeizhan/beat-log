import {EventEmitter} from "events";
import * as path from "path";
import * as fs from "fs";
import {DBHelper} from "../db_helper";
import * as es from "event-stream";
import {FileWorker, FileWorkerEvent} from "./file_worker";

export enum ReadWorkerEvent {
    UHDB_ENOUGH = "uhdb_enough",                // 读取uhdb足够数量的行数可以进行操作
    NORMAL_ENOUGH = "normal_enough",            // 读取文件足够数量的行数可以进行操作
    NORMAL_ALL_FINISH = "normal_all_finish",    // 完成读取所有文件
    UHDB_ALL_FINISH = "uhdb_all_finish"        // 完成读取uhdb所有数据
}

/**
 * 配置类
 */
export interface ReadWorkerOptions {
    /**
     * 日志目录
     */
    logDir: string;
    /**
     * 文件工作对象
     */
    fileWorker: FileWorker;
    /**
     * 一次读取日志行数
     */
    batchLineNum?: number;
    /**
     * uhdb对象
     */
    uhdb: any;
}

/**
 * 读取数据工作类
 *
 * enough: 读取到足够的数据事件
 */
export class ReadWorker extends EventEmitter {

    private _batchLineNum: number;
    private _logDir: string;
    private _uhdbKeyStream: any;
    private _lineGate: any;

    private _uhdb;
    private _fileWorker: FileWorker;
    private _uhdbFinished: boolean;
    private _isIdle: boolean = true;
    /**
     * 一个文件读取完成
     */
    private _oneFileFinished: boolean;

    constructor(options: ReadWorkerOptions) {
        super();

        if (!options.logDir || !path.isAbsolute(options.logDir)) {
            throw new SyntaxError(`日志路径必须为绝对路径:${options.logDir}`);
        }

        this._batchLineNum = options.batchLineNum || 10;
        this._logDir = options.logDir;
        this._fileWorker = options.fileWorker;

        this._uhdb = options.uhdb;
        this._uhdbFinished = false;
        this._oneFileFinished = false;

        this._fileWorker.on(FileWorkerEvent.FILE_READY, readFilePath => {
            this._readLogs(fs.createReadStream(readFilePath));
        });

        this._fileWorker.on(FileWorkerEvent.FILE_ALL_FINISH, () => {
            console.log("触发NORMAL_FINISH事件");
            this._isIdle = true;
            this.emit(ReadWorkerEvent.NORMAL_ALL_FINISH);
            // 再次启动监控
            this._monitorOnceLogDir();
        });
    }

    /**
     * 是否在空闲状态
     * @returns {boolean}
     */
    public isIdle():boolean {
        return this._isIdle;
    }

    public onStart() {
        console.log("启动读取日志程序");
        this._isIdle = false;
        this._readUhdb();
    }

    /**
     * 读取未处理的数据
     * @private
     */
    private _readUhdb(): void {
        let tmpArr: Array<string> = [];

        this._uhdbFinished = false;

        this._uhdbKeyStream = this._uhdb.createKeyStream({encoding: "utf8"});

        this._uhdbKeyStream.on("data", data => {
            tmpArr.push(data);

            if (tmpArr.length >= this._batchLineNum) {
                this._uhdbKeyStream.pause();

                this.emit(ReadWorkerEvent.UHDB_ENOUGH, tmpArr);

                // 数据清理
                tmpArr = [];
            }
        }).on("end", () => {

            this._uhdbFinished = true;

            if (tmpArr != null && tmpArr.length > 0) {
                // 数据处理
                this.emit(ReadWorkerEvent.UHDB_ENOUGH, tmpArr);
            } else {
                this._uhdbFinishedWork();
            }

        }).on("error", err => {
            console.error(`读取uhdb数据库出错:${err}`);
        });
    }

    private _uhdbFinishedWork() {
        this.emit(ReadWorkerEvent.UHDB_ALL_FINISH);

        console.log("触发UHDB_FINISH事件");
        this._fileWorker.onStart();
    }

    /**
     * 监控gsLog日志目录
     * 只触发一次，写入所有数据后重新监控
     * 避免重复触发
     */
    private _monitorOnceLogDir() {
        console.log("启动gsLog日志监控");
        let logWatcher = fs.watch(this._logDir, (evt, fileName) => {
            if (evt === "change") {

                logWatcher.close();

                // 避免立马读取导致文件打开出错
                process.nextTick(() => {
                    this._fileWorker.onStart();
                });
            }
        });
    }


    /**
     * 批量读取日志文件行
     * @param {"fs".ReadStream} ms
     * @private
     */
    private _readLogs(ms: fs.ReadStream) {

        let lineArray: Array<string> = [];

        this._oneFileFinished = false;

        this._lineGate = es.pause();

        ms.pipe(es.split(/\r?\n/))
            .pipe(this._lineGate)
            .pipe(es.mapSync(line => {

                // 长度至少30
                if (line == null || line.length < 30) {
                    // TODO:避免(sliced string)
                    // line = null;
                    return;
                }

                lineArray.push(line);
                if (lineArray.length >= this._batchLineNum) {
                    this._lineGate.pause();
                    this.emit(ReadWorkerEvent.NORMAL_ENOUGH, lineArray);
                    // 数据清理
                    lineArray = [];
                }
            }))
            .pipe(es.wait(() => {
                this._oneFileFinished = true;

                if (lineArray && lineArray.length > 0) {
                    this.emit(ReadWorkerEvent.NORMAL_ENOUGH, lineArray);
                } else {
                    this._fileWorker.onReadNextFile();
                }
            }));
    }

    resumeReadLine() {
        if (this._lineGate == null) {
            console.warn(`resumeReadLine():this._gate为空`);
            return;
        }

        if (this._oneFileFinished) {
            this._fileWorker.onReadNextFile();
        } else {
            this._lineGate.resume();
        }
    }

    pauseReadLine() {
        if (this._lineGate == null) {
            console.warn(`pauseReadLine():this._gate实例为空`);
            return;
        }

        this._lineGate.pause();
    }

    resumeUhdbKeyStream() {
        if (this._uhdbKeyStream == null) {
            console.warn("resumeUhdbKeyStream():this._uhdbKeyStream为空");
            return;
        }

        if (this._uhdbFinished) {
            this._uhdbFinishedWork();
        } else {
            this._uhdbKeyStream.resume();
        }
    }
}
