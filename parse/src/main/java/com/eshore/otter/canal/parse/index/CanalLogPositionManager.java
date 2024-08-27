package com.eshore.otter.canal.parse.index;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.eshore.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Created by yinxiu on 17/3/17. Email: marklin.hz@gmail.com
 */
public interface CanalLogPositionManager extends CanalLifeCycle {

    LogPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;

}
