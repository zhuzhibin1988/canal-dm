# debezium-dameng

#### 介绍
达梦数据库同步工具，使用基于日志的CDC
当前版本数据类型转换有bug，暂时仅支持varcahr类型，后续修复

#### 软件架构
基于debezium 1.5.4final和达梦的日志解析工具API

###### 本地测试
1.达梦日志配置
```
--创建同步账户
CREATE USER TEST IDENTIFIED BY "syncdm.";
GRANT DBA TO TEST;
--开启兼容模式
SP_SET_PARA_VALUE(2,'compatible_mode',2);

SELECT para_name,para_type,para_value FROM V$DM_INI WHERE PARA_NAME = 'COMPATIBLE_MODE';
--开启归档日志
ALTER DATABASE MOUNT;
ALTER DATABASE ADD ARCHIVELOG 'DEST = /opt/dmdbms/data/DAMENG/archivelog , TYPE = local, FILE_SIZE = 1024, SPACE_LIMIT = 2048';
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
--开启闪回
SP_SET_PARA_VALUE(1,'ENABLE_FLASHBACK',1);
SELECT para_name,para_value FROM V$DM_INI WHERE PARA_NAME ='ENABLE_FLASHBACK';
```

2.启动测试类，并输出结果

![img.png](../../doc/img.png)

#### 集成到Flink CDC
文档后续补充







# dameng-cdc
