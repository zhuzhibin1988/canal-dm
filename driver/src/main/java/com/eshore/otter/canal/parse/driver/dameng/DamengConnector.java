/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.eshore.otter.canal.parse.driver.dameng;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <pre>
 *  <ul>达梦jdbc连接器
 *      <li>classloader加载jdbc驱动类</li>
 *      <li>创建connection，并测试连接状态</li>
 *      <li>执行sql查询</li>
 *  </ul>
 * </pre>
 */

public class DamengConnector implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnector.class);


    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");


    private static final String JDBC_PATTERN = "jdbc:dm//${hostname}:${port}";

    private static final String JDBC_DRIVER = "dm.jdbc.driver.DmDriver";

    /**
     * The database version.
     */
    private final DamengDatabaseVersion databaseVersion;


    private Properties prop;
    private ConnectionFactory factory;
    private Connection conn;


    public DamengConnector(InetSocketAddress socketAddress,
                           String username,
                           String password) {

        this.prop = new Properties();
        this.prop.put(DamengJdbcConfiguration.HOSTNAME, socketAddress.getHostName());
        this.prop.put(DamengJdbcConfiguration.PORT, socketAddress.getPort());
        this.prop.put(DamengJdbcConfiguration.USERNAME, username);
        this.prop.put(DamengJdbcConfiguration.PASSWORD, password);
        this.factory = prepareConnectionFactory();

        this.databaseVersion = resolveDmDatabaseVersion();
        LOGGER.info("Database Version: {}", databaseVersion.getBanner());
    }


    @FunctionalInterface
    @ThreadSafe
    public interface ConnectionFactory {
        Connection getConnection(Properties prop) throws SQLException;
    }

    private synchronized ConnectionFactory prepareConnectionFactory() {
        return (prop) -> {
            ClassLoader classLoader = DamengConnector.class.getClassLoader();
            Connection conn = null;
            try {
                Class<Driver> driverClass = (Class<Driver>) Class.forName(JDBC_DRIVER, true, classLoader);
                Driver driver = driverClass.getDeclaredConstructor().newInstance();
                conn = driver.connect(findAndReplace(JDBC_PATTERN, prop), prop);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("dameng driver is not found, check classpath include driver lib or not");
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
            return conn;
        };
    }

    private String findAndReplace(String urlPattern, Properties prop) {
        String url = urlPattern;
        for (Map.Entry kv : prop.entrySet()) {
            String key = kv.getKey().toString();
            String value = kv.getValue().toString();
            if (urlPattern.contains("${" + key + "}")) {
                url = url.replaceAll("\\$\\{" + key + "}", value);
            }
        }
        return url;
    }

    public synchronized Connection connect() throws SQLException {
        if (isConnected()) {
            return this.conn;
        } else {
            this.conn = this.factory.getConnection(this.prop);
        }
        return this.conn;
    }

    public synchronized boolean isConnected() throws SQLException {
        if (this.conn == null) {
            return false;
        }
        return !this.conn.isClosed();
    }

    public synchronized void disconnect() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
        }
    }

    public synchronized void reconnect() throws SQLException {
        disconnect();
        connect();
    }

    public InetAddress getAddress() {
        InetSocketAddress socketAddress = new InetSocketAddress(this.prop.getProperty(DamengJdbcConfiguration.HOSTNAME),
                Integer.parseInt(this.prop.getProperty(DamengJdbcConfiguration.PORT)));
        return socketAddress.getAddress();
    }

    private DamengDatabaseVersion resolveDmDatabaseVersion() {
        String versionStr = null;
        try {
            // Oracle 18.1 introduced BANNER_FULL as the new column rather than BANNER
            // This column uses a different format than the legacy BANNER.
            ResultSet rs = connect().createStatement().executeQuery("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'DM Database%'");
            if (rs.next()) {
                versionStr = rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to resolve dm database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve dm database version");
        }

        return DamengDatabaseVersion.parse(versionStr);
    }

    @Override
    public void close() throws Exception {
        disconnect();
    }

}
