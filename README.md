# 日志发送工具
能够配置一次性读取日志文件日志行数,然后对每一行日志进行处理,并且备份log文件.另外,单文件读取方便优雅关闭程序.
## 版本需求
- node>=6.0
- npm>=3.0
- typescript>=2.5
## 使用方法
- 配置
  - config/monitor.json
    - logDir: 日志路径
    - bakDir: 备份日志路径
    - bakSize: 备份大小,配合bakSizeUnit使用
    - bakSizeUnit: 备份大小单位,比如:"MB","GB".
  - (可选) config/logstash.json
- 编译全部ts文件
- 启动
```npm
npm start
```