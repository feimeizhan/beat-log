# 日志发送工具
能够配置一次性读取日志文件个数和日志行数,然后对每一行日志进行处理,并且备份log文件.
项目目的是实现Elastic里面的[filebeat](https://www.elastic.co/products/beats/filebeat)程序不具备的文件迁移和备份功能,[详情](https://github.com/elastic/beats/issues/714).
可能使用分支[single-file-handler](https://github.com/feimeizhan/beat-log/tree/single-file-handler)更加少bug
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