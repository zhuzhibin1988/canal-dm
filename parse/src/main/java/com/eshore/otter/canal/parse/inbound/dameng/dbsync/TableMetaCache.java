package com.eshore.otter.canal.parse.inbound.dameng.dbsync;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;

import com.eshore.otter.canal.parse.inbound.dameng.DamengConnection;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 处理table meta解析和缓存
 *
 * @author jianghang 2013-1-17 下午10:15:16
 * @version 1.0.0
 */
public class TableMetaCache {

    public static final String COLUMN_NAME = "field";
    public static final String COLUMN_TYPE = "type";
    public static final String IS_NULLABLE = "null";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_DEFAULT = "default";
    public static final String EXTRA = "extra";
    private DamengConnection connection;
    private boolean isOnRDS = false;
    private boolean isOnPolarX = false;
    private boolean isOnTSDB = false;

    private TableMetaTSDB tableMetaTSDB;
    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private LoadingCache<String, TableMeta> tableMetaDB;

    public TableMetaCache(DamengConnection con, TableMetaTSDB tableMetaTSDB) {
        this.connection = con;
        this.tableMetaTSDB = tableMetaTSDB;
        // 如果持久存储的表结构为空，从db里面获取下
        if (tableMetaTSDB == null) {
            this.tableMetaDB = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {

                @Override
                public TableMeta load(String name) throws Exception {
                    try {
                        return getTableMetaByDB(name);
                    } catch (Throwable e) {
                        // 尝试做一次retry操作
                        try {
                            connection.reconnect();
                            return getTableMetaByDB(name);
                        } catch (IOException e1) {
                            throw new CanalParseException("fetch failed by table meta:" + name, e1);
                        }
                    }
                }

            });
        } else {
            isOnTSDB = true;
        }
    }

    private synchronized TableMeta getTableMetaByDB(String fullname) throws SQLException {
        String[] names = StringUtils.split(fullname, "`.`");
        String schema = names[0];
        String table = names[1].substring(0, names[1].length());
        ResultSet rs = connection.query(LogMinerSqls.descTableStatement(schema, table));
        return new TableMeta(schema, table, parseTableMeta(schema, table, rs));
    }

    public static List<FieldMeta> parseTableMeta(String schema, String table, ResultSet rs) {
        List<FieldMeta> fieldMetas = new ArrayList<>();
        try {
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String columnType = rs.getString("column_type");
                FieldMeta fieldMeta = new FieldMeta();
                fieldMeta.setColumnName(columnName);
                fieldMeta.setColumnType(columnType);
                fieldMetas.add(fieldMeta);
            }
        } catch (SQLException e) {

        }
        return fieldMetas;
    }


    public TableMeta getTableMeta(String schema, String table) {
        return getTableMeta(schema, table, true);
    }

    public TableMeta getTableMeta(String schema, String table, boolean useCache) {
        if (!useCache) {
            tableMetaDB.invalidate(getFullName(schema, table));
        }

        return tableMetaDB.getUnchecked(getFullName(schema, table));
    }

    public TableMeta getTableMeta(String schema, String table, EntryPosition position) {
        return getTableMeta(schema, table, true, position);
    }

    public synchronized TableMeta getTableMeta(String schema, String table, boolean useCache, EntryPosition position) {
        TableMeta tableMeta = null;
        if (tableMetaTSDB != null) {
            tableMeta = tableMetaTSDB.find(schema, table);
            if (tableMeta == null) {
                // 因为条件变化，可能第一次的tableMeta没取到，需要从db获取一次，并记录到snapshot中
                String fullName = getFullName(schema, table);
                ResultSet rs = null;
                String createDDL = null;
                try {
                    try {
                        LogMinerSqls.
                        rs = connection.query("show create table " + fullName);
                    } catch (Exception e) {
                        // 尝试做一次retry操作
                        connection.reconnect();
                        rs = connection.query("show create table " + fullName);
                    }
                    if (packet.getFieldValues().size() > 0) {
                        createDDL = packet.getFieldValues().get(1);
                    }
                    // 强制覆盖掉内存值
                    tableMetaTSDB.apply(position, schema, createDDL, "first");
                    tableMeta = tableMetaTSDB.find(schema, table);
                } catch (IOException e) {
                    throw new CanalParseException("fetch failed by table meta:" + fullName, e);
                }
            }
            return tableMeta;
        } else {
            if (!useCache) {
                tableMetaDB.invalidate(getFullName(schema, table));
            }

            return tableMetaDB.getUnchecked(getFullName(schema, table));
        }
    }

    public void clearTableMeta(String schema, String table) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidate(getFullName(schema, table));
        }
    }

    public void clearTableMetaWithSchemaName(String schema) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            for (String name : tableMetaDB.asMap().keySet()) {
                if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                    // removeNames.add(name);
                    tableMetaDB.invalidate(name);
                }
            }
        }
    }

    public void clearTableMeta() {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidateAll();
        }
    }

    /**
     * 更新一下本地的表结构内存
     *
     * @param position
     * @param schema
     * @param ddl
     * @return
     */
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        if (tableMetaTSDB != null) {
            return tableMetaTSDB.apply(position, schema, ddl, extra);
        } else {
            // ignore
            return true;
        }
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
                .append(schema)
                .append('`')
                .append('.')
                .append('`')
                .append(table)
                .append('`')
                .toString();
    }

    public boolean isOnTSDB() {
        return isOnTSDB;
    }

    public void setOnTSDB(boolean isOnTSDB) {
        this.isOnTSDB = isOnTSDB;
    }
}
