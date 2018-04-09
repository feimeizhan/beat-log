import * as fs from "fs";
import * as path from "path";

const level = require("level");

export enum DBExitCode {

    RDB_READ_ERR = -101,
    /**
     * 插入已读日志DB出错
     */
    RDB_INNSERT_ERR = -100,
    /**
     * 插入无法处理数据DB出错
     * @type {number}
     */
    UHDB_INNSERT_ERR = -200
}


export class DBHelper {

    /**
     * 已读日志文件DB路径
     * key:日志文件名称
     * value:日志绝对路径
     * @type {string}
     */
    static readFileDBDir: string = path.join(__dirname, "/data/rdb");

    /**
     * 无法处理日志文件的行内容DB路径
     * key:日志原始行内容
     * value:文件名称
     * @type {string}
     */
    static unHandleDBDir: string = path.join(__dirname, "/data/uhdb");

    private static _uhdb;
    private static _rdb;

    /**
     * 简单单例模式
     * @param {string} dir
     * @return {any}
     */
    static getDB(dir: string) {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir)
        }

        if (dir === DBHelper.readFileDBDir) {
            if (this._rdb == null) {
                this._rdb = level(dir, {keyEncoding: "utf8mb4", valueEncoding: "utf8mb4"});
            }
            return this._rdb;
        }

        if (dir === DBHelper.unHandleDBDir) {
            if (this._uhdb == null) {
                this._uhdb = level(dir, {keyEncoding: "utf8mb4", valueEncoding: "utf8mb4"});
            }
            return this._uhdb;
        }

        return level(dir, {keyEncoding: "utf8mb4", valueEncoding: "utf8mb4"});
    }

    /**
     *
     * @return {levelup.LevelUp}
     */
    static getUnHandleDB() {
        return DBHelper.getDB(DBHelper.unHandleDBDir);
    }

    static getReadFileDB() {
        return DBHelper.getDB(DBHelper.readFileDBDir);
    }

}
