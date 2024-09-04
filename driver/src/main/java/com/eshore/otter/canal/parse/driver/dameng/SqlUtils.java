/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.otter.canal.parse.driver.dameng;

import com.eshore.dbsync.logmnr.Scn;
import com.google.common.base.Strings;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This utility class contains SQL statements to configure, manage and query Oracle LogMiner
 *     todo handle INVALID file member (report somehow and continue to work with valid file), handle adding multiplexed files,
 *     todo SELECT name, value FROM v$sysstat WHERE name = 'redo wastage';
 *     todo SELECT GROUP#, STATUS, MEMBER FROM V$LOGFILE WHERE STATUS='INVALID'; (drop and recreate? or do it manually?)
 *     todo table level supplemental logging
 *     todo When you use the SKIP_CORRUPTION option to DBMS_LOGMNR.START_LOGMNR, any corruptions in the redo log files are skipped during select operations from the V$LOGMNR_CONTENTS view.
 *     todo if file is compressed?
 * // For every corrupt redo record encountered,
 * // a row is returned that contains the value CORRUPTED_BLOCKS in the OPERATION column, 1343 in the STATUS column, and the number of blocks skipped in the INFO column.
 */
public class SqlUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);
    // ****** RAC specifics *****//
    // https://docs.oracle.com/cd/B28359_01/server.111/b28319/logminer.htm#i1015913
    // https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:18183400346178753
    // We should never read from GV$LOG, GV$LOGFILE, GV$ARCHIVED_LOG, GV$ARCHIVE_DEST_STATUS and GV$LOGMNR_CONTENTS
    // using GV$DATABASE is also misleading
    // Those views are exceptions on RAC system, all corresponding V$ views see entries from all RAC nodes.
    // So reading from GV* will return duplications, do no do it
    // *****************************

    // database system views
    private static final String DATABASE_VIEW = "V$ARCH_FILE";
    private static final String LOGFILE_VIEW = "V$LOGFILE";
    private static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";

    // LogMiner statements
//    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    public static final String SELECT_SYSTIMESTAMP = "SELECT SYSTIMESTAMP FROM DUAL";
    public static final String END_LOGMNR = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";

    /**
     * Querying V$LOGMNR_LOGS
     * After a successful call to DBMS_LOGMNR.START_LOGMNR, the STATUS column of the V$LOGMNR_LOGS view contains one of the following values:
     * 0
     * Indicates that the redo log file will be processed during a query of the V$LOGMNR_CONTENTS view.
     * 1
     * Indicates that this will be the first redo log file to be processed by LogMiner during a select operation against the V$LOGMNR_CONTENTS view.
     * 2
     * Indicates that the redo log file has been pruned and therefore will not be processed by LogMiner during a query of the V$LOGMNR_CONTENTS view.
     * It has been pruned because it is not needed to satisfy your requested time or SCN range.
     * 4
     * Indicates that a redo log file (based on sequence number) is missing from the LogMiner redo log file list.
     */
    public static final String FILES_FOR_MINING = "SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS";

    public static String databaseSupplementalLoggingAllCheckQuery() {

        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
    }

    public static String databaseSupplementalLoggingMinCheckQuery() {

        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM %s", DATABASE_VIEW);
    }

    public static String currentScnQuery() {
        return String.format("SELECT ARCH_LSN FROM %s", DATABASE_VIEW);
    }

    public static String oldestFirstChangeQuery(Duration archiveLogRetention) {
        final StringBuilder sb = new StringBuilder();
        // sb.append("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        // sb.append("FROM ").append(LOG_VIEW).append(" ");
        // sb.append("UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# ");
        // sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        // sb.append("WHERE DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery()).append(") ");
        // sb.append("AND STATUS='A'");
        // if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
        // sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
        // }
        //
        // return sb.append(")").toString();
        sb.append("SELECT ARCH_LSN FROM SYS.V$ARCH_FILE ORDER BY CREATE_TIME LIMIT 1");
        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24)");
        }

        return sb.toString();
    }

    public static String allOnlineLogsQuery() {
        return String.format("SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP#, L.FIRST_CHANGE# AS FIRST_CHANGE, L.STATUS " +
                " FROM %s L, %s F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE#, L.STATUS ORDER BY 3", LOG_VIEW, LOGFILE_VIEW);
    }

    /**
     * Obtain the query to be used to fetch archive logs.
     *
     * @param scn                 oldest scn to search for
     * @param archiveLogRetention duration archive logs will be mined
     * @return query
     */
    public static String archiveLogsQuery(Scn scn, Duration archiveLogRetention) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" ");
        sb.append("WHERE NAME IS NOT NULL ");
        sb.append("AND ARCHIVED = 'YES' ");
        sb.append("AND STATUS = 'A' ");
        sb.append("AND NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery()).append(") ");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24) ");
        }

        return sb.append("ORDER BY 2").toString();
    }

    // ***** LogMiner methods ***

    /**
     * This returns statement to build LogMiner view for online redo log files
     *
     * @param startScn mine from
     * @param endScn   mine till
     * @param strategy Log Mining strategy
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    public static String startLogMinerStatement(Scn startScn, Scn endScn) {
        return "BEGIN dbms_logmnr.start_logmnr( startScn => '" + startScn + "',  endScn => '" + endScn + "', OPTIONS => 2128 ); END;";
    }

    /**
     * This is the query from the LogMiner view to get changes.
     * <p>
     * The query uses the following columns from the view:
     * <pre>
     * SCN - The SCN at which a change was made
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * OPERATION_CODE - Number of the operation code
     * TIMESTAMP - Timestamp when the database change was made
     * XID - Transaction Identifier
     * CSF - Continuation SQL flag, identifies rows that should be processed together as a single row (0=no,1=yes)
     * TABLE_NAME - Name of the modified table
     * SEG_OWNER - Schema/Tablespace name
     * OPERATION - Database operation type
     * USERNAME - Name of the user who executed the transaction
     * </pre>
     *
     * @param connectorConfig the connector configuration
     * @param logMinerUser    log mining session user name
     * @return the query
     */
    public static String logMinerContentsQuery(String logMinerUser) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");
        query.append("WHERE ");
        query.append("SCN > ? AND SCN <= ? ");
        query.append("AND (");
        // MISSING_SCN/DDL only when not performed by excluded users
        query.append("(OPERATION_CODE IN (5,34) AND USERNAME NOT IN (").append(getExcludedUsers(logMinerUser)).append(")) ");
        // COMMIT/ROLLBACK
        query.append("OR (OPERATION_CODE IN (7,36)) ");
        // INSERT/UPDATE/DELETE
        query.append("OR ");
        query.append("(OPERATION_CODE IN (1,2,3) ");

        // There are some common schemas that we automatically ignore when building the filter predicates
        // and we pull that same list of schemas in here and apply those exclusions in the generated SQL.
        if (!DamengConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            query.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = DamengConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext(); ) {
                String excludedSchema = i.next();
                query.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(") ");
        }

        String schemaPredicate = buildSchemaPredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = buildTablePredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");

        return query.toString();
    }

    private static String getExcludedUsers(String logMinerUser) {
        return "'SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'";
    }

    private static String buildSchemaPredicate(DamengConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.schemaIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.schemaExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", true)).append(")");
            }
        } else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", false)).append(")");
        }
        return predicate.toString();
    }

    private static String buildTablePredicate(DamengConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.tableIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.tableExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", true)).append(")");
            }
        } else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", false)).append(")");
        }
        return predicate.toString();
    }

    private static String listOfPatternsToSql(List<Pattern> patterns, String columnName, boolean applyNot) {
        StringBuilder predicate = new StringBuilder();
        for (Iterator<Pattern> i = patterns.iterator(); i.hasNext(); ) {
            Pattern pattern = i.next();
            if (applyNot) {
                predicate.append("NOT ");
            }
            // NOTE: The REGEXP_LIKE operator was added in Oracle 10g (10.1.0.0.0)
            final String text = resolveRegExpLikePattern(pattern);
            predicate.append("REGEXP_LIKE(").append(columnName).append(",'").append(text).append("','i')");
            if (i.hasNext()) {
                // Exclude lists imply combining them via AND, Include lists imply combining them via OR?
                predicate.append(applyNot ? " AND " : " OR ");
            }
        }
        return predicate.toString();
    }

    private static String resolveRegExpLikePattern(Pattern pattern) {
        // The REGEXP_LIKE operator acts identical to LIKE in that it automatically prepends/appends "%".
        // We need to resolve our matches to be explicit with "^" and "$" if they don't already exist so
        // that the LIKE aspect of the match doesn't mistakenly filter "DEBEZIUM2" when using "DEBEZIUM".
        String text = pattern.pattern();
        if (!text.startsWith("^")) {
            text = "^" + text;
        }
        if (!text.endsWith("$")) {
            text += "$";
        }
        return text;
    }

    public static String addLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => " + option + ");END;";
    }

    public static String deleteLogFileStatement(String fileName) {

        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
    }

    public static String tableExistsQuery(String tableName) {

        return "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = '" + tableName + "'";
    }

    public static String dropTableStatement(String tableName) {
        
        return "DROP TABLE " + tableName.toUpperCase() + " PURGE";
    }

    // no constraints, no indexes, minimal info
    public static String logMiningHistoryDdl(String tableName) {
        return "create  TABLE " + tableName + "(" +
                "row_sequence NUMBER(19,0), " +
                "captured_scn NUMBER(19,0), " +
                "table_name VARCHAR2(30 CHAR), " +
                "seg_owner VARCHAR2(30 CHAR), " +
                "operation_code NUMBER(19,0), " +
                "change_time TIMESTAMP(6), " +
                // "row_id VARCHAR2(20 CHAR)," +
                // "session_num NUMBER(19,0)," +
                // "serial_num NUMBER(19,0)," +
                "transaction_id VARCHAR2(50 CHAR), " +
                // "rs_id VARCHAR2(34 CHAR)," +
                // "ssn NUMBER(19,0)," +
                "csf NUMBER(19,0), " +
                "redo_sql VARCHAR2(4000 CHAR)" +
                // "capture_time TIMESTAMP(6)" +
                ") nologging";
    }

    public static String truncateTableStatement(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    /**
     * This method return query which converts given SCN in days and deduct from the current day
     */
    public static String diffInDaysQuery(Scn scn) {
        if (scn == null) {
            return null;
        }
        return "select sysdate - CAST(scn_to_timestamp(" + scn.toString() + ") as date) from dual";
    }

    public static String descTableStatement(String schema, String tableName) {
        return String.format("select "
                        + "owner, table_name, column_name, data_type as column_type "
                        + "from all_tab_columns "
                        + "where  owner = upper('%s') and  table_name = upper('%s')",
                schema, tableName);
    }
}
