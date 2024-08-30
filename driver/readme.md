# driver核心功能流程
1. 通过jdbc连接dameng数据库
2. 查询归档日志，返回归档文件，位置，状态信息
   +SELECT PATH,ARCH_LSN,CLSN,STATUS FROM SYS.V$ARCH_FILE;
3. 加载归档文件到dameng的日志挖掘
   + dbms_logmnr.add_logfile(LOGFILENAME => 'E:\data\dm\arch\ARCHIVE_LOCAL1_0x210E2400_EP0_2024-08-23_11-16-55.log',options => DBMS_LOGMNR.ADDFILE);
4. 启动dameng的日志挖掘
   + dbms_logmnr.start_logmnr( startScn => 43717,  endScn => 43800, OPTIONS => 2128 );
5. 通过offset位置偏移查询redo_sql
   + SELECT * FROM V$LOGMNR_CONTENTS
      WHERE SCN > ? AND SCN <= ?
      AND ((OPERATION_CODE  IN (5, 34) AND SEG_OWNER NOT IN ('SYS', 'SYSTEM', 'SYSDBA'))
      OR (OPERATION_CODE IN (7, 36))
      OR (OPERATION_CODE IN (1, 2, 3)
      AND TABLE_NAME != 'LOG_MINING_FLUSH'
      AND SEG_OWNER NOT IN ('APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP',
      'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS',
      'ORDDATA', 'ORDSYS', 'OUTLN', 'SYS', 'SYSTEM', 'WMSYS', 'XDB')))
      ORDER BY SCN;
6. 结束当前归档文件挖掘
   - dbms_logmnr.end_logmnr();
7. 查询下一个归档文件，重复3，4，5，6
# driver核心接口的输入输出
+ 输入：传入偏移量，批大小
+ 输出：LogArchive，最新LogPosition对象