package com.jtljia.pump;

import com.taobao.tddl.dbsync.binlog.LogEvent;

public interface Sink{

	 public boolean sink(LogEvent event);

}
