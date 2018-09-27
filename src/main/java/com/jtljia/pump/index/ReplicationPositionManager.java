package com.jtljia.pump.index;

import com.jtljia.pump.canal.protocol.position.ReplicationPosition;
import com.jtljia.pump.ServiceLifeCycle;
import com.jtljia.pump.exception.ParseException;

/**
 * 接口组合
 */
public interface ReplicationPositionManager extends ServiceLifeCycle {

	ReplicationPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, ReplicationPosition logPosition) throws ParseException;
}
