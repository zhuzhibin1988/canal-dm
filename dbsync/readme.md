# dbsync核心功能基本流程
1. fetch归档日志，
2. 读取归档日志的redo sql，解析sql内容，
    + WriterRowLogEvent
    + UpdateRowLogEvent
    + DeleteRowLogEvent
    + TableDDLLogEvent

# dbsync调用过程
+ fetch调用driver获取LogArchive