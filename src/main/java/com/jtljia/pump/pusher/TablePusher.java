package com.jtljia.pump.pusher;

import com.jtljia.pump.Tuple4;
import com.jtljia.pump.canal.protocol.CanalEntry;
import com.jtljia.pump.canal.protocol.CanalEntry.Column;
import com.jtljia.pump.canal.protocol.CanalEntry.EventType;
import com.jtljia.pump.canal.protocol.CanalEntry.RowChange;
import com.jtljia.pump.canal.protocol.CanalEntry.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class TablePusher {
	
	private Logger logger = LoggerFactory.getLogger( TablePusher.class );
	
	private String tableName;
	
	private String targetTableName;
	
	private ToMySQLPusher parent;
	
	private PreparedStatement insertStatement;

	private PreparedStatement updateStatement;

	private long timestamp;

	//                  target  source  target是否主键       映射函数
	private List<Tuple4<String, String, Boolean, Function<List<Column>, String>>> columnMap = new ArrayList<>();

	//需要修改的字段
	private List<Tuple4<String, String, Boolean, Function<List<Column>, String>>> updateColumnMap = new ArrayList<>();
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public List<Tuple4<String, String, Boolean, Function<List<Column>, String>>> getColumnMap() {
		return columnMap;
	}
	
	public void setParent(ToMySQLPusher parent) {
		this.parent = parent;
	}

	private PreparedStatement getInsertStatement() throws SQLException {
		timestamp = System.currentTimeMillis();
		insertStatement = parent.getConnection().prepareStatement( getInsertSQL() );
		return insertStatement;
	}

	private PreparedStatement getUpdateStatement() throws SQLException {
		timestamp = System.currentTimeMillis();
		updateStatement = parent.getConnection().prepareStatement( getUpdateSQL() );
		return updateStatement;
	}

	private boolean checkDbConnection() throws SQLException {
		if(System.currentTimeMillis() < timestamp+3600000){
			return true;
		}
		try {
			parent.connectionTarget();
			return true;
		}catch (Exception e){
			throw new SQLException("connect to targetConnection failed",e);
		}
	}
	
	public boolean pushToTarget(CanalEntry.Entry entry) throws SQLException {
		RowChange rowChage = null;
		try {
			rowChage = RowChange.parseFrom(entry.getStoreValue());
		} catch (Exception e) {
			throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
		}
		EventType eventType = rowChage.getEventType();
		if( eventType == EventType.DELETE ) {
			return true;
		}
		if( rowChage.getIsDdl() ) {
			return true;
		}	
		
		for (RowData rowData : rowChage.getRowDatasList()) {
			StringBuilder sb = new StringBuilder().append("pump data success :").append( tableName ).append( "->" ).append( targetTableName ).append(":");
			
			List<Column> columns = rowData.getAfterColumnsList();

			getUpdatedColumns(columns);

			checkDbConnection();
			int updateInt = 0;
			StringBuilder updateSb = new StringBuilder();
			StringBuilder updateWhereSb = new StringBuilder().append(" where ");
			if(updateColumnMap.size() > 0){
				PreparedStatement updatePS = getUpdateStatement();
				int whereParamCount = 0;

				for( int i=0; i<updateColumnMap.size(); i++ ) {
					Tuple4<String, String, Boolean, Function<List<Column>, String>> map = updateColumnMap.get(i);
					String value = getMappedValue(map, columns);
					updatePS.setString( i+1, value );
					updateSb.append(" update ").append( map.e2() ).append( ":" ).append( value ).append( "->" ).append( map.e1() ).append( ":" ).append( value ).append( ", " );
				}
				for( int i=0; i<columnMap.size(); i++ ) {
					Tuple4<String, String, Boolean, Function<List<Column>, String>> map = columnMap.get(i);
					String value = getMappedValue(map, columns);
					if( map.e3() ) {
						updatePS.setString( updateColumnMap.size()+(++whereParamCount), value);
						updateWhereSb.append(map.e1()).append(" = ").append(value);
					}
				}
				updateInt = updatePS.executeUpdate();
			}

			if( updateInt < 1 && updateColumnMap.size() > 0){
				PreparedStatement insertPS = getInsertStatement();
				StringBuilder insertSb = new StringBuilder();
				for( int i=0; i<columnMap.size(); i++ ) {
					Tuple4<String, String, Boolean, Function<List<Column>, String>> map = columnMap.get(i);

					String value = getMappedValue( map, columns);

					insertPS.setString( i+1, value );
					insertSb.append(" insert ").append( map.e2() ).append( ":" ).append( value ).append( "->" ).append( map.e1() ).append( ":" ).append( value ).append( ", " );
				}
				insertPS.executeUpdate();
				logger.info( sb.append(insertSb).toString() );
			}else if(updateInt > 0){
				logger.info( sb.append(updateSb).append(updateWhereSb).toString() );
			}
		}
		return true;
		
	}
	
	public void close(){
		//close statement
		try {
			if (insertStatement != null){
				insertStatement.close();
			}
			if (updateStatement != null){
				updateStatement.close();
			}
		} catch (SQLException e) {
			logger.error("ERROR # close preparedStatement failed", e);
		}
	}
	
	private String getInsertSQL() {
		StringBuilder sb = new StringBuilder();
		sb.append( "insert ignore into ").append( targetTableName ).append( "(" );
		columnMap.forEach( t->sb.append(t.e1()).append(",") );
		sb.deleteCharAt( sb.length()-1 ).append(") values( ");;
		columnMap.forEach( t->sb.append('?').append(",") );
		sb.deleteCharAt( sb.length()-1 ).append(")");
		return sb.toString();
	}

	private String getUpdateSQL() {
		StringBuilder update   = new StringBuilder("UPDATE ").append( targetTableName ).append( " SET " );
		StringBuilder where = new StringBuilder();
		updateColumnMap.forEach( t->{
			update.append( t.e1() ).append("=?").append(",");
		} );
		columnMap.forEach(t->{
			if( t.e3() ) {
				if( where.length()>0 )
					where.append( " and ");
				where.append( t.e1() ).append( "=?");
			}
		});
		
		update.deleteCharAt( update.length()-1 ).append(" WHERE ").append( where.toString() );
		return update.toString();
	}
	
	private String getMappedValue(Tuple4<String, String, Boolean, Function<List<Column>, String>> map, 
			                      List<Column> sourceColumns) {
		String value; 
		if( map.e4()!=null ) {
			value = map.e4().apply(sourceColumns);
		} else {
			Column sc = sourceColumns.stream().filter( c->c.getName().equals( map.e2() ) ).findFirst().get();
			value = sc.getValue();
			if( value!=null && value.isEmpty() && !spaceAble( sc.getSqlType() )){
				value = null;
			}
		}
		return value;
	}

	private void getUpdatedColumns(List<Column> sourceColumns){
		updateColumnMap.clear();
		sourceColumns.stream().forEach(sc->{
			if(sc.getUpdated()){
				columnMap.forEach(c->{
					//特殊处理 元组2为null并且元组4部位null（syn_lms_house的region_id）
					if((null != c.e2() && c.e2().equals(sc.getName())) || (null == c.e2() && null != c.e4())){
						updateColumnMap.add(c);
					}
				});
			}
		});
	}
	
	private boolean spaceAble(int jdbcType){
		return jdbcType==Types.CHAR || jdbcType==Types.VARCHAR ||jdbcType==Types.LONGVARCHAR  ||jdbcType==Types.CLOB
			||jdbcType==Types.NCHAR  ||jdbcType==Types.NVARCHAR ||jdbcType==Types.LONGNVARCHAR||jdbcType==Types.NCLOB
			||jdbcType==Types.SQLXML;
	}
}
