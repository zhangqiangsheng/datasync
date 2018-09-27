package com.jtljia.pump.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jtljia.pump.canal.protocol.position.ReplicationPosition;
import com.jtljia.pump.AbstractServiceLifeCycle;

/**
 * 基于内存的实现
 */
public class MemoryPositionManager extends AbstractServiceLifeCycle implements ReplicationPositionManager {

    protected Map<String, ReplicationPosition> positions;

    public void doStart() {
        positions = new ConcurrentHashMap<>();
    }

    public void doStop() {
        positions.clear();
    }

    public ReplicationPosition getLatestIndexBy(String destination) {
        return positions.get(destination);
    }

    public void persistLogPosition(String destination, ReplicationPosition logPosition) {
        positions.put(destination, logPosition);
    }
}
