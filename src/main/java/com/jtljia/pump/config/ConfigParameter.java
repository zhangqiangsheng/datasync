package com.jtljia.pump.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * 配置参数
 */
public class ConfigParameter{
	
	// 网络链接参数
	public String  instanceIP       = Config.factory.getInstance().get("source.instance.ip").get();
	public Integer instancePort     = Config.factory.getInstance().getInt("source.instance.port").orElse(3306);
	public String  instanceUsername = Config.factory.getInstance().get("source.instance.username").get(); // 数据库用户
	public String  instancePassword = Config.factory.getInstance().get("source.instance.password").get(); // 数据库密码
	public String  instanceDb        = Config.factory.getInstance().get("source.instance.db").get(); // 原数据库
	
	public String targetInstanceIp       = Config.factory.getInstance().get("target.instance.ip").get();
	public Integer targetInstancePort    = Config.factory.getInstance().getInt("target.instance.port").get();
	public String targetInstanceUsername = Config.factory.getInstance().get("target.instance.username").get();
	public String targetInstancePassword = Config.factory.getInstance().get("target.instance.password").get();
	public String targetInstanceDb        = Config.factory.getInstance().get("target.instance.db").get();
	
	// 编码信息
	public Byte    connectionCharsetNumber = (byte) 33;
	public String  connectionCharset = "UTF-8";
	public Integer receiveBufferSize = 8 * 1024;
	public Integer sendBufferSize    = 8 * 1024;
	public Integer defaultConnectionTimeoutInSeconds = 30; // sotimeout
	
	public Integer slaveId = Config.factory.getInstance().getInt("slave.id").orElse(2000);
	
	public String  positionManagerDir = Config.factory.getInstance().get("position.file.dir").get();

	// 心跳检查信息
	public String  detectingSQL = Config.factory.getInstance().get("source.instance.detectingSQL").orElse(""); // 心跳sql
	public Integer detectingIntervalInSeconds = 10; // 检测频率
	
	public Boolean filterQueryDcl          = false;
	public Boolean filterQueryDml          = false;
	public Boolean filterQueryDdl          = false;
	public Boolean filterRows              = false;
	public Boolean filterTableError = Boolean.FALSE; // 是否忽略表解析异常
	
	public String[] filterTables = {};
	
	public ConfigParameter() {
		String logfileName = Config.factory.getInstance().get("source.instance.start.logfileName").orElse(null);
		String logfileOffest = Config.factory.getInstance().get("source.instance.start.logfileOffest").orElse(null);;
		String logfileTimestamp = Config.factory.getInstance().get("source.instance.start.logfileTimestamp").orElse("");;
		
		if( StringUtils.isNotBlank( logfileName ) ) {
			buildPosition(logfileName, Long.valueOf(logfileOffest), Long.valueOf(logfileTimestamp));
		}
		
		String tables = Config.factory.getInstance().get("source.instance.tables").orElse("");
		if( !tables.isEmpty() ) {
			filterTables = tables.split(",");
		}
	}
	
	private String buildPosition(String journalName, Long position, Long timestamp) {
		StringBuilder masterBuilder = new StringBuilder();
		if (StringUtils.isNotEmpty(journalName) || position != null || timestamp != null) {
			masterBuilder.append('{');
			if (StringUtils.isNotEmpty(journalName)) {
				masterBuilder.append("\"journalName\":\"").append(journalName).append("\"");
			}

			if (position != null) {
				if (masterBuilder.length() > 1) {
					masterBuilder.append(",");
				}
				masterBuilder.append("\"position\":").append(position);
			}

			if (timestamp != null) {
				if (masterBuilder.length() > 1) {
					masterBuilder.append(",");
				}
				masterBuilder.append("\"timestamp\":").append(timestamp);
			}
			masterBuilder.append('}');
			return masterBuilder.toString();
		} else {
			return null;
		}
	}
}
