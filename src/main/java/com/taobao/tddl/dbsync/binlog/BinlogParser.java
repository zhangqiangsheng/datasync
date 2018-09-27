package com.taobao.tddl.dbsync.binlog;

import com.jtljia.pump.canal.protocol.CanalEntry;
import com.jtljia.pump.exception.ParseException;

/**
 * 解析binlog的接口
 */
public interface BinlogParser<T> {

    CanalEntry.Entry parse(T event) throws ParseException;

    void reset();
}
