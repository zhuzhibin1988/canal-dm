# canal-dm

# 启用logmnr redo日志功能
## 开启日志追加模式
select arch_mode from v$database;
select para_name, para_value from v$dm_ini where para_name in ('ARCH_INI','RLOG_APPEND_LOGIC');
sp_set_para_value(2,'RLOG_APPEND_LOGIC',1); 1：会话生效，2：全局生效
sp_set_para_value(2,'LOGMNR_PARSE_LOB',1);  归档文件clob解析
## CDC 功能依赖于数据库的归档日志模式
### 开启日志模式
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD ARCHIVELOG 'DEST = /data/dameng/data/DAMENG_ARCHIVE, TYPE = local, FILE_SIZE = 32, SPACE_LIMIT =10240';
ALTER DATABASE OPEN;
### 关闭日志模式
ALTER DATABASE MOUNT;
ALTER DATABASE NOARCHIVELOG;
ALTER DATABASE DELETE ARCHIVELOG 'dest=/data/dm8/data/DAMENG_ARCHIVE';
ALTER DATABASE OPEN;
## 配置闪回功能以支持 CDC 操作
SP_SET_PARA_VALUE(1,'ENABLE_FLASHBACK',1);
SELECT para_name,para_value FROM V$DM_INI WHERE PARA_NAME ='ENABLE_FLASHBACK';