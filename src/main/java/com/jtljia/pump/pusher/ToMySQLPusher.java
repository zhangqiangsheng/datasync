package com.jtljia.pump.pusher;

import com.jtljia.pump.AuthenticationInfo;
import com.jtljia.pump.canal.protocol.CanalEntry;
import com.jtljia.pump.canal.protocol.CanalEntry.EntryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author wujie
 */
public class ToMySQLPusher{
	private static Logger logger =  LoggerFactory.getLogger( ToMySQLPusher.class );
	
	private String[] filterTables = {}; 
	
	private AuthenticationInfo targetInfo;
	
	private Connection targetConnection;
	
	private List<TablePusher> tablePusherList = null;
	
	public List<TablePusher> getTablePusherList() {
		return tablePusherList;
	}

	public void setTablePusherList(List<TablePusher> tablePusherList) {
		this.tablePusherList = tablePusherList;
	}

	public ToMySQLPusher( AuthenticationInfo targetInfo ) throws SQLException {
		this.targetInfo = targetInfo;
		connectionTarget();
	}
	
	public void setFilterTables(String[] filterTables) {
		this.filterTables = filterTables;
	}

	public Connection getConnection() throws SQLException {
		if( targetConnection==null || !targetConnection.isValid(5) )
			connectionTarget();
		return targetConnection;
	}
	
	public void connectionTarget() throws SQLException {
		if( targetConnection!=null )
			targetConnection.close();
		targetConnection = DriverManager.getConnection( "jdbc:mysql:/"+targetInfo.getAddress().toString(), 
                                                         targetInfo.getUsername(), targetInfo.getPassword() );
	}
	
	public boolean push(CanalEntry.Entry event) throws SQLException {
		if( filter(event ) ) {
			return doPush(event);
		} else {
			return true;
		}
	}
	
	public void close() {
		try{
			if(tablePusherList != null && tablePusherList.size()>0){
				tablePusherList.forEach( t->t.close() );
			}
			if(targetConnection != null){
				targetConnection.close();
			}
			logger.info("service resources are normally closed.");
		}catch (Exception e){
			logger.error("ERROR # close targetConnection or tablePusher failed", e);
		}

	}

	public boolean filter(String tableName){
		for( int i=0; i<filterTables.length; i++ ) {
			if( filterTables[i].equals( tableName ) ) {
				return true;
			}
		}
		return false;
	}

	private boolean filter(CanalEntry.Entry event) {
		if (event.getEntryType() == EntryType.ROWDATA) {
			return filter(getSchemaNameAndTableName(event));
		} 
		return false;
	}

	private String getSchemaNameAndTableName(CanalEntry.Entry entry) {
		return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
	}
	
	private boolean doPush(CanalEntry.Entry entry) throws SQLException {
		for( TablePusher tp : tablePusherList ) {
			if( getSchemaNameAndTableName(entry).equals( tp.getTableName() ) ) { 
				return tp.pushToTarget( entry );
			}
		}
		return true;
	}
}
