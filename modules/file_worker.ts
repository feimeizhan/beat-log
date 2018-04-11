import {EventEmitter} from "events";
import * as path from "path";
import * as fs from "fs";
import {DBExitCode, DBHelper} from "../db_helper";

export enum FileWorkerEvent {
    FILE_ALL_FINISH = "file_all_finish",    // 文件全部去取完毕
    FILE_READY = "file_ready"             // 一个文件读取准备就绪
}

export enum FileReadOrder {
    /**
     * 不排序
     */
    NONE = 0,
    /**
     * 修改时间升序
     */
    M_TIME_ASC,
    /**
     * 修改时间降序
     */
    M_TIME_DESC
}

/**
 * 配置类
 */
export interface FileWorkerOptions {
    /**
     * 日志目录
     */
    logDir: string;
    /**
     * 备份日志目录
     */
    bakLogDir: string;
    /**
     * 备份临界点
     */
    bakSize?: number;
    /**
     * 备份临界点单位,比如:"MB"
     */
    bakSizeUnit?: string;
    /**
     * rdb对象
     */
    rdb: any;
}

export class FileWorker extends EventEmitter {

    private _fileNameList: Array<string>;
    /**
     * 当前已读文件个数
     */
    private _currReadFileCount: number;
    /**
     * 当前读取文件名称
     */
    private _currReadFileName: string;
    /**
     * 从onStart()到FILE_ALL_FINISH事件触发
     * 读取所有文件耗时
     */
    private _readExecTimestamp: number;
    private _rdb;
    private _logDir: string;

    private _bakSize: number;
    private _bakSizeUnit: string;
    private _bakLogDir: string;

    constructor(options: FileWorkerOptions) {
        super();

        this._logDir = options.logDir;
        this._rdb = options.rdb;
        this._fileNameList = [];
        this._bakSize = options.bakSize || 100;
        this._bakSizeUnit = options.bakSizeUnit || "MB";
        this._bakLogDir = options.bakLogDir;
    }

    public onStart(order: FileReadOrder = FileReadOrder.M_TIME_ASC) {

        // 统计数据初始化
        this._readExecTimestamp = Date.now();
        this._currReadFileCount = 0;

        if (this._fileNameList == null) {
            this._fileNameList = [];
        }

        // 检查当前目录是否有文件还没有读取
        // 遍历当前日志目录下的所有文件
        let tmpKeys: Array<string> = [];
        this._rdb.createKeyStream({encoding: "utf8"}).on("data", data => {
            tmpKeys.push(data);
        }).on("end", () => {
            fs.readdirSync(this._logDir).forEach(fileName => {

                if (tmpKeys.indexOf(fileName) === -1) {
                    this._fileNameList.push(fileName);
                } else {
                    console.log(`忽略已读文件${fileName}`);
                }
            });


            this._fileNameList = this._fileNameList.filter(this._filterLogFileName, this._logDir);

            // 文件排序
            this._fileNameList = this._sortFile(this._logDir, this._fileNameList, order);

            this._readFile();

        }).on("error", err => {
            console.error(`读取rdb数据库出错:${err}`);

            process.exit(DBExitCode.RDB_READ_ERR);
        });
    }

    /**
     * 按照文件的各种条件,条件需要是数值属性等排序
     * @param {string} dir
     * @param {Array<string>} fileNameList
     * @param {FileReadOrder} order
     * @return {Array<string>}
     * @private
     */
    private _sortFile(dir: string, fileNameList: Array<string>, order: FileReadOrder): Array<string> {
        if (fileNameList == null) {
            throw new SyntaxError(`fileNameList不能为空`);
        }

        if (dir == null) {
            throw new SyntaxError(`dir不能为空`);
        }

        let property: string;
        let isAsc: boolean;

        switch (order) {
            case FileReadOrder.NONE:
                return fileNameList;
            case FileReadOrder.M_TIME_ASC:
                isAsc = true;
                property = "mtimeMs";
                break;
            case FileReadOrder.M_TIME_DESC:
                property = "mtimeMs";
                isAsc = false;
                break;
            default:
                throw new SyntaxError(`文件排序参数不正确:${order}`);
        }

        return fileNameList.map(fileName => {
            return {
                name: fileName,
                property: fs.statSync(path.join(dir, fileName))[property]
            };
        }).sort((a, b) => {
            return isAsc ? b[property] - a[property] : a[property] - b[property];
        }).map(item => {
            return item.name;
        });
    }

    /**
     * 读取下一个文件
     */
    public onReadNextFile() {
        this._rdb.put(this._currReadFileName, Date.now()).then(() => {
            console.log(`已读日志文件${this._currReadFileName}入库成功`);
            this._currReadFileCount++;
            this._readFile();
        }).catch(err => {
            if (err) {
                console.error(`已读日志文件名入库失败:${err}`);
                // 避免疯狂重读
                process.exit(DBExitCode.RDB_INNSERT_ERR);
            }
        });
    }

    private _readFile() {
        if (this._fileNameList == null || this._fileNameList.length === 0) {
            console.log("触发FILE_ALL_FINISH事件");

            // 判断是否需要备份日志文件
            // 默认单位为MB
            if (this._isNeedBak(this._logDir, this._bakSize * 1024 * 1024)) {
                console.log("准备备份日志文件");

                this._bakLogFile(this._logDir, this._bakLogDir, err => {
                    if (err) {
                        console.error(`备份日志文件失败`);
                    }
                });
            } else {
                console.log("不需要备份日志文件");
            }

            this.emit(FileWorkerEvent.FILE_ALL_FINISH, this._currReadFileCount, Date.now() - this._readExecTimestamp);
        } else {
            console.log("触发FILE_ENOUGH事件");

            this._currReadFileName = this._fileNameList.shift();

            this.emit(FileWorkerEvent.FILE_READY, path.join(this._logDir, this._currReadFileName));
        }
    }

    /**
     * 备份文件
     * @param {string} logDir
     * @param {string} bakDir
     * @param {Function} cb 成功备份的回调函数
     * @private
     */
    private _bakLogFile(logDir: string, bakDir: string, cb: (any) => void) {
        let count: number = 0;
        let sucFiles: Array<string> = [];
        // 获取已读文件列表
        this._rdb.createKeyStream({encoding: "utf8"}).on("data", data => {
            // 移动已读文件到指定备份目录
            fs.renameSync(path.join(logDir, data), path.join(bakDir, `${data}.${Date.now()}.bak`));
            sucFiles.push(data);
            count++;
        }).on("end", () => {
            // 备份完毕后处理
            console.log(`备份完毕,总工备份文件:${count}个`);
            this._delFinishBakFiles(sucFiles);
            // 删除已备份文件
            cb(null);
        }).on("error", err => {
            console.error(`备份日志文件出错:${err},成功备份文件:${count}个`);
            this._delFinishBakFiles(sucFiles);
            cb(err);
        });
    }

    private _delFinishBakFiles(fls: Array<string>) {

        if (fls == null || fls.length === 0) {
            console.log("没有备份成功的文件");
            return;
        }

        this._rdb.batch(fls.map(filename => {
            return {
                type: "del", key: filename
            };
        })).then(() => {
            console.log(`清理rdb里已备份文件成功:${fls.length}个`);
        }).catch(err => {
            if (err) {
                console.log(`清理rdb里已备份文件失败:${err}`);
                process.exit(-1);
            }
        });
    }

    /**
     * 判断当前文件夹的文件需不需要备份
     * @param {string} dir
     * @param {number} bakSize 单位: Byte
     * @return {boolean}
     * @private
     */
    private _isNeedBak(dir: string, bakSize: number): boolean {

        let sumSize: number = 0;

        fs.readdirSync(dir).forEach(filename => {
            // 过滤不是日志文件
            if (path.extname(filename) !== ".log") {
                return;
            }

            let tmpStat = fs.statSync(path.join(dir, filename));
            if (!tmpStat.isFile()) {
                return;
            }

            sumSize += tmpStat.size;
        });

        console.log(`文件夹${dir}的大小:${sumSize / 1024}KB`);

        return bakSize <= sumSize;
    }

    /**
     * 过滤不是日志的文件名
     * @param {string} fileName
     * @returns {boolean}
     * @private
     */
    private _filterLogFileName(fileName: string) {
        // 过滤掉不是日志文件
        if (path.extname(fileName) !== ".log") {
            console.log(`${fileName}文件不是日志文件不做处理`);
            return false;
        }

        let p = path.join(this.toString(), fileName);
        if (!fs.existsSync(p)) {
            console.log(`不存在文件:${p}`);
            return false;
        }

        return true;
    }

}
