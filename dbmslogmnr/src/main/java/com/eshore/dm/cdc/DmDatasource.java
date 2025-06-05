package com.eshore.dm.cdc;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/5/30 14:21
 * @Description: TODO
 */
public class DmDatasource {
    private final DataSource dataSource;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DmDatasource(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.dataSource = buildDruidDataSource();
    }

    /**
     * 构建并配置 Druid 数据源
     *
     * @return 配置好的 DataSource 实例
     */
    private DataSource buildDruidDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();

        // 设置数据库连接信息，请根据实际环境修改
        druidDataSource.setUrl(this.jdbcUrl); // JDBC URL
        druidDataSource.setUsername(this.username);                    // 数据库用户名
        druidDataSource.setPassword(this.password);                    // 数据库密码
        druidDataSource.setDriverClassName("dm.jdbc.driver.DmDriver");  // 达梦驱动类名

        // 基础连接池配置
        druidDataSource.setInitialSize(5);         // 初始连接数
        druidDataSource.setMinIdle(5);             // 最小空闲连接
        druidDataSource.setMaxActive(20);          // 最大连接数
        druidDataSource.setMaxWait(60000);         // 获取连接最大等待时间（毫秒）
        druidDataSource.setValidationQuery("SELECT 1"); // 验证SQL
        druidDataSource.setTestWhileIdle(true);    // 空闲时验证连接有效性
        druidDataSource.setTestOnBorrow(false);    // 不在borrow时检测
        druidDataSource.setTestOnReturn(false);    // 不在return时检测

        return druidDataSource;
    }

    public Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
