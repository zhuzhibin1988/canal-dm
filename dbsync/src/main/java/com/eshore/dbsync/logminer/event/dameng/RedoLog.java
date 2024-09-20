package com.eshore.dbsync.logminer.event.dameng;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2024/8/30 16:01
 * @Description: TODO
 */
public class RedoLog {
    private String redoSql;
    private Operation operation;
    private Scn scn;
    private String xid;
    private String schema;
    private String tableName;
    private String rowId;
    private Scn cscn;

    public enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    private RedoLog(String redoSql, Operation operation,
                    Scn scn, String xid, String schema,
                    String tableName, String rowId, Scn cscn) {
        this.redoSql = redoSql;
        this.operation = operation;
        this.scn = scn;
        this.xid = xid;
        this.schema = schema;
        this.tableName = tableName;
        this.rowId = rowId;
        this.cscn = cscn;
    }

    public String getRedoSql() {
        return this.redoSql;
    }

    public Operation getOperation() {
        return this.operation;
    }

    public Scn getScn() {
        return this.scn;
    }

    public String getXid() {
        return this.xid;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getRowId() {
        return this.rowId;
    }

    public Scn getCscn() {
        return this.cscn;
    }

    public static class Builder {
        private String redoSql;
        private String operation;
        private String scn;
        private String xid;
        private String database;
        private String tableName;
        private String rowId;
        private String cscn;

        public static Builder create() {
            return new Builder();
        }

        public void setRedoSql(String redoSql) {
            this.redoSql = redoSql;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public void setScn(String scn) {
            this.scn = scn;
        }

        public void setXid(String xid) {
            this.xid = xid;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setRowId(String rowId) {
            this.rowId = rowId;
        }

        public void setCscn(String cscn) {
            this.cscn = cscn;
        }

        public RedoLog build() {
            return new RedoLog(
                    this.redoSql,
                    Operation.valueOf(this.operation.toUpperCase()),
                    Scn.valueOf(this.scn),
                    xid,
                    database,
                    tableName,
                    rowId,
                    Scn.valueOf(cscn)
            );
        }
    }
}
