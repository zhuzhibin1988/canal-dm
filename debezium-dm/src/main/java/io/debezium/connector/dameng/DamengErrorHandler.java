/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.sql.SQLRecoverableException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handle for Oracle.
 *
 * @author Chris Cranford
 */
public class DamengErrorHandler extends ErrorHandler {

    public DamengErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(DamengConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable.getMessage() == null || throwable.getCause() == null) {
            return false;
        }

        return throwable.getMessage().startsWith("ORA-03135") || // connection lost
                throwable.getMessage().startsWith("ORA-12543") || // TNS:destination host unreachable
                throwable.getMessage().startsWith("ORA-00604") || // error occurred at recursive SQL level 1
                throwable.getMessage().startsWith("ORA-01089") || // Oracle immediate shutdown in progress
                throwable.getMessage().startsWith("ORA-01333") || // Failed to establish LogMiner dictionary
                throwable.getMessage().startsWith("ORA-01284") || // Redo/Archive log cannot be opened, likely locked
                throwable.getMessage().startsWith("ORA-26653") || // Apply DBZXOUT did not start properly and is currently in state INITIALI
                throwable.getMessage().startsWith("ORA-01291") || // missing logfile
                throwable.getCause() instanceof IOException ||
                throwable instanceof SQLRecoverableException ||
                throwable.getMessage().toUpperCase().contains("NO MORE DATA TO READ FROM SOCKET");
        // ||
        // (throwable.getCause() != null && throwable.getCause().getCause() instanceof NetException);
    }
}
