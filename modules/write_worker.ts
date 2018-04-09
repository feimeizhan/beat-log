import {EventEmitter} from "events";
import {DBExitCode, DBHelper} from "../db_helper";
import * as path from "path";
import * as fs from "fs";
import * as es from "event-stream";

export enum WriteWorkerEvent {
    NORMAL_BATCH_FINISH = "batch_finish",       // 一次批量日志读取完毕
    CHANGE_MODULE = "change_module",            // 模块有修改事件
    UH_BATCH_FINISH = "uh_finish"               // 没有处理的日志全部处理完毕事件
}


export class WriteWorker extends EventEmitter {
    /**
     * 未处理日志的原始行记录的数据库
     */
    private _moduleDir: string;
    private _cachedModule: any = {};
    private _gameName: string;

    private _modulePrefix: string = "f_logstash";

    private _batchLineNum: number;
    private _uhdb;

    constructor(gameName: string, batchLineNum: number, moduleDir: string = "impl") {
        super();

        this._batchLineNum = batchLineNum;
        this._moduleDir = path.join(__dirname, moduleDir);
        this._gameName = gameName;
        this._uhdb = DBHelper.getUnHandleDB();

        this._loadAllModules();
    }

    /**
     * 加载所有模块
     */
    private _loadAllModules() {
        fs.readdirSync(this._moduleDir).filter((fileName) => {
            return path.extname(fileName) === ".js";
        }).forEach(fileName => {
            this._cachedModule[`${path.basename(fileName, ".js")}`] =
                require(path.join(this._moduleDir, fileName)).default;
        });

        this._monitorOnceModuleDir();
    }

    /**
     * 监控模块目录
     */
    private _monitorOnceModuleDir() {
        let moduleDirWatcher = fs.watch(this._moduleDir, evt => {
            if (evt === "change") {
                moduleDirWatcher.close();

                this._cachedModule = {};
                this._loadAllModules();
            }
        });
    }

    /**
     * 避免过多操作阻塞时间片，导致数据库操作超时
     * @param {Array<string>} lineArray
     * @param {boolean} isRetry
     * @private
     */
    batchHandleLine(lineArray: Array<string>, isRetry: boolean = false) {
        // 每次读取1000条数据
        if (lineArray == null || lineArray.length === 0) {
            return;
        }

        es.readArray(lineArray).pipe(es.map((data, cb) => {
            this._handleLine(data, isRetry).then(() => {
                cb(null, data);
            }).catch(err => {
                // 正常情况下不可达
                // 因为this._handleLine()方法捕获了异常
                cb(err);
            });
        })).pipe(es.wait(() => {
            console.log(`一次批量处理行数据成功,处理行数:${lineArray.length}`);
            if (isRetry) {
                this.emit(WriteWorkerEvent.UH_BATCH_FINISH);
            } else {
                this.emit(WriteWorkerEvent.NORMAL_BATCH_FINISH);
            }
        }));
    }

    /**
     * 处理日志文件的原始行数据
     * @param {string} line
     * @param {boolean} isRetry 是否重试,默认为非重试
     */
    private async _handleLine(line: string, isRetry: boolean = false) {
        if (line == null || line.trim() == "") {
            console.log("空行不做处理");
            return;
        }

        try {
            let dealFuncName: string = `${this._modulePrefix}`;
            let dealFunc: Function = this._cachedModule[dealFuncName];

            if (typeof dealFunc === "function") {
                // 异步处理
                await dealFunc(line);
                console.log(`${line}:处理成功`);
                // 如果是重试数据需要更新数据库
                if (!isRetry) {
                    return;
                }
                this._delUhdb(line);
            } else {
                let errMsg: string = `不存在处理函数:${dealFuncName}`;

                console.log(errMsg);
                if (isRetry) {
                    return;
                }

                // 存储没法处理的数据
                this._putUhdb(line, errMsg);
            }
        } catch (e) {
            let errMsg: string = `处理行数据(${line})出错:${e}`;
            console.error(errMsg);

            if (isRetry) {
                return;
            }

            this._putUhdb(line, errMsg);
        }
    }

    private _delUhdb(line: string): void {
        this._uhdb.del(line).then(() => {
            console.log(`${line}:删除成功`);
        }).catch(err => {
            console.error(`${line}删除失败:${err}`);
        });
    }

    private _putUhdb(line: string, err: string) {
        this._uhdb.put(line, err).then(() => {
            console.log(`${line}:未处理数据入库成功`);
        }).catch(err => {
            if (err) {
                console.error(`没法处理日志行记录入库失败:${err}`);
                // 避免疯狂入库
                process.exit(DBExitCode.UHDB_INNSERT_ERR);
            }
        });
    }
}
