import * as es from "event-stream";
import {MapStream} from "event-stream";
import {EventEmitter} from "events";
import * as path from "path";
import * as fs from "fs";
import {DBExitCode, DBHelper} from "../db_helper";

export enum FileWorkerEvent {
    FILE_ALL_FINISH = "file_all_finish",    // 文件全部去取完毕
    FILE_ENOUGH = "file_enough"             // 一次批量文件读取完毕
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

export class FileWorker extends EventEmitter {

    private _fileNameList: Array<string>;
    private _tmpReadFileList: Array<string>;
    private _rdb;
    private _logDir: string;
    private _batchFileNum: number;

    private _bakSize: number;
    private _bakSizeUnit: string;
    private _bakLogDir: string;

    constructor(logDir: string, batchFileNum: number = 100,
                bakSize: number = 100, bakSizeUnit: string = "MB", bakLogDir: string) {
        super();

        this._logDir = logDir;
        this._batchFileNum = batchFileNum;
        this._rdb = DBHelper.getReadFileDB();
        this._fileNameList = [];
        this._bakSize = bakSize;
        this._bakSizeUnit = bakSizeUnit;
        this._bakLogDir = bakLogDir;
    }

    public onStart(order: FileReadOrder = FileReadOrder.M_TIME_ASC) {

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

            // 文件排序
            this._fileNameList = this._sortFile(this._logDir, this._fileNameList, order);

            this._readBatchFile();

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

    public onReadBatchFile() {

        if (this._tmpReadFileList == null || this._tmpReadFileList.length == 0) {
            this._readBatchFile();
        } else {
            let opts = [];

            this._tmpReadFileList.forEach(value => {
                opts.push({type: "put", key: value, value: new Date()});
            });

            this._rdb.batch(opts).then(() => {
                console.log(`已读日志文件${this._tmpReadFileList.length}个入库成功`);
                this._readBatchFile();
            }).catch(err => {
                if (err) {
                    console.error(`已读日志文件名入库失败:${err}`);
                    // 避免疯狂重读
                    process.exit(DBExitCode.RDB_INNSERT_ERR);
                }
            });
        }

    }

    private _readBatchFile() {
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

            this.emit(FileWorkerEvent.FILE_ALL_FINISH);
        } else {
            console.log("触发FILE_ENOUGH事件");
            this._tmpReadFileList = this._fileNameList.splice(0, this._batchFileNum);

            this._tmpReadFileList = this._tmpReadFileList.filter(this._filterFileName, this._logDir);

            // 避免被修改
            let tmpReadFileList: Array<string> = JSON.parse(JSON.stringify(this._tmpReadFileList));

            this.emit(FileWorkerEvent.FILE_ENOUGH, tmpReadFileList, this._mergeStream(this._tmpReadFileList));
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

    private _filterFileName(fileName: string) {
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

    private _mergeStream(fileNameList): MapStream {

        if (fileNameList == null || fileNameList.length === 0) {
            console.log("没有需要合并的文件流");
            return es.merge();
        }

        return es.merge(fileNameList.map(fileName => {
            // 过滤掉不是日志文件
            if (path.extname(fileName) !== ".log") {
                console.log(`${fileName}文件不是日志文件不做处理`);
                return;
            }

            let p = path.join(this._logDir, fileName);
            if (!fs.existsSync(p)) {
                console.log(`不存在文件:${p}`);
                return;
            }

            return fs.createReadStream(p);
        }).filter(stream => {
            return stream != null;
        }));
    }

}
