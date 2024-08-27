package com.eshore.otter.canal.parse.driver.mysql.utils;

import com.eshore.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.eshore.otter.canal.parse.driver.mysql.packets.MariaGTIDSet;
import com.eshore.otter.canal.parse.driver.mysql.packets.MysqlGTIDSet;

/**
 * 类 GtidUtil.java 的实现
 *
 * @author winger 2020/9/24 1:25 下午
 * @version 1.0.0
 */
public class GtidUtil {

    public static GTIDSet parseGtidSet(String gtid, boolean isMariaDB) {
        if (isMariaDB) {
            return MariaGTIDSet.parse(gtid);
        } else {
            return MysqlGTIDSet.parse(gtid);
        }
    }
}
