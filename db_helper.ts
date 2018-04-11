import * as fs from "fs";
import * as mkdirp from "mkdirp";

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

/**
 * 数据库类型
 */
export enum DB_TYPE {
    RDB_TYPE = 1,
    UHDB_TYPE = 2
}


export class DBHelper {

    private static _uhdb = {};
    private static _rdb = {};

    /**
     * 简单单例模式
     * @param {DB_TYPE} dbType
     * @param {string} dir 存储数据目录
     * @return {any}
     */
    static getDB(dbType:DB_TYPE, dir: string) {
        if (!fs.existsSync(dir)) {
            mkdirp.sync(dir);
        }

        switch (dbType) {
            case DB_TYPE.RDB_TYPE:
                if (this._rdb[dir] == null) {
                    this._rdb[dir] = level(dir, {keyEncoding: "utf8mb4", valueEncoding: "utf8mb4"});
                }

                return this._rdb[dir];
            case DB_TYPE.UHDB_TYPE:
                if (this._uhdb[dir] == null) {
                    this._uhdb[dir] = level(dir, {keyEncoding: "utf8mb4", valueEncoding: "utf8mb4"});
                }
                return this._uhdb[dir];
            default:
                throw new RangeError(`超出DB_TYPE范围:${dbType}`);
        }
    }

    /**
     * 获取未处理数据
     * @return {levelup.LevelUp}
     */
    static getUnHandleDB(dir: string) {
        return DBHelper.getDB(DB_TYPE.UHDB_TYPE, dir);
    }

    /**
     * 获取未读文件
     * @param {string} dir
     * @returns {any | any}
     */
    static getReadFileDB(dir: string) {
        return DBHelper.getDB(DB_TYPE.RDB_TYPE, dir);
    }

}
