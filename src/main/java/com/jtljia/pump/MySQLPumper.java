package com.jtljia.pump;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jtljia.pump.canal.parse.driver.mysql.MySQLConnector;
import com.jtljia.pump.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.jtljia.pump.canal.parse.driver.mysql.packets.HeaderPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.jtljia.pump.canal.parse.driver.mysql.utils.PacketManager;
import com.jtljia.pump.canal.protocol.CanalEntry;
import com.jtljia.pump.canal.protocol.position.EntryPosition;
import com.jtljia.pump.canal.protocol.position.ReplicationPosition;
import com.jtljia.pump.canal.protocol.position.SlaveIdentity;
import com.jtljia.pump.exception.ParseException;
import com.jtljia.pump.index.ReplicationPositionManager;
import com.jtljia.pump.pusher.ToMySQLPusher;
import com.taobao.tddl.dbsync.binlog.BinlogParser;
import com.taobao.tddl.dbsync.binlog.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * 
 * @author felix.wu
 *
 */
public class MySQLPumper{
    private static final Logger logger  = LoggerFactory.getLogger(MySQLPumper.class);
    private static final long BINLOG_START_OFFEST     = 4L;
   
    private int                fallbackIntervalInSeconds  = 60;       // 切换回退时间
    
    private boolean            needTransactionPosition = false;
    
    private MySQLConnector     connector; 
   
    private long               slaveId;

    private ReplicationPositionManager positionManager    = null;
    private BinlogParser<LogEvent> binlogParser = null;
    private ToMySQLPusher pusher;
    private MysqlQueryExecutor  queryExecutor;
    
    public MySQLPumper(){}

    public MySQLPumper( InetSocketAddress address, String username, String password, 
    		            byte charsetNumber, String defaultSchema){
        connector = new MySQLConnector(address, username, password, charsetNumber, defaultSchema);
        queryExecutor = new MysqlQueryExecutor(connector);
    }
    
    public void setPositionManager(ReplicationPositionManager positionManager) {
		this.positionManager = positionManager;
	}
	public void setBinlogParser(BinlogParser<LogEvent> binlogParser) {
		this.binlogParser = binlogParser;
	}
	public void setPusher(ToMySQLPusher pusher) {
		this.pusher = pusher;
	}
	public void connect() throws IOException {
        connector.connect();
    }
    public void reconnect() throws IOException {
        connector.reconnect();
    }
    public void disconnect() throws IOException {
        connector.disconnect();
    }
    public boolean isConnected() {
        return connector.isConnected();
    }
    public MysqlQueryExecutor getQueryExecutor() {
		return queryExecutor;
	}
    public long getSlaveId() {
        return slaveId;
    }
    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }
    public MySQLConnector getConnector() {
        return connector;
    }
    public void setConnector(MySQLConnector connector) {
        this.connector = connector;
    }
	public boolean isNeedTransactionPosition() {
		return needTransactionPosition;
	}
	public void setNeedTransactionPosition(boolean needTransactionPosition) {
		this.needTransactionPosition = needTransactionPosition;
	}
	/**
     * 加速主备切换时的查找速度，做一些特殊优化，比如只解析事务头或者尾
     */
    public void seek(String binlogfilename, Long binlogPosition, Sink func) throws IOException {
        updateSettings();
        sendBinlogDump(binlogfilename, binlogPosition);
        
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new ParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    public void dump()throws IOException, SQLException{
    	connector.connect();// 链接
        // 重新链接，因为在找position过程中可能有状态，需要断开后重建
        final EntryPosition startPosition = findStartPosition();
        if (startPosition == null)
            throw new ParseException("can't find start position for destination");
        logger.info("find start position : {}", startPosition);
        connector.reconnect();
    	updateSettings();
    	
        sendBinlogDump(startPosition.getJournalName(), startPosition.getPosition());
        
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        while( !Thread.interrupted() && fetcher.fetch()  ) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);          
            if (event == null) {
                throw new ParseException("parse failed");
            }

            if( event instanceof WriteRowsLogEvent ){
                TableMapLogEvent table = ((WriteRowsLogEvent)event).getTable();
                if (table != null) {
                    if( !pusher.filter( table.getDbName() + "." + table.getTableName() ) )
                        continue;
                }
            }

            CanalEntry.Entry entry = binlogParser.parse(event);
            if (entry != null) {
            	if( pusher.push(entry) )
            		positionManager.persistLogPosition(connector.getAddress().toString(), buildLastPosition(entry));
            	else {
            		logger.error( "pusher failed for {} will interrupt", entry.getHeader().getTableName() );
            		break;
            	}
            }
        }
        
        logger.info("exit dump");
    }
    
    private EntryPosition findStartPosition() throws IOException {
    	EntryPosition startPosition = findStartPositionInternal();
    	if (needTransactionPosition) {
             logger.warn("prepare to find last position : {}", startPosition.toString());
             Long preTransactionStartPosition = findTransactionBeginPosition(startPosition);
             if ( !preTransactionStartPosition.equals(startPosition.getPosition()) ) {
                 logger.warn("find new start Transaction Position , old : {} , new : {}",startPosition.getPosition(), preTransactionStartPosition);
                 startPosition.setPosition(preTransactionStartPosition);
             }
        }
    	return startPosition;
    }
    
    private EntryPosition findStartPositionInternal() {
		ReplicationPosition logPosition = positionManager.getLatestIndexBy(connector.getAddress().toString());
	    if (logPosition == null) {// 找不到历史成功记录
           EntryPosition entryPosition = findEndPosition(); // 默认从当前最后一个位置进行消费

           if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
               // 如果指定binlogName + offest，直接返回
               logger.warn("prepare to find start position {}:{}:{}", entryPosition.getJournalName(), entryPosition.getPosition(), "" );
               return entryPosition;
           } else {
               EntryPosition specificLogFilePosition = null;
               if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                   // 如果指定binlogName +
                   // timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                   logger.warn("prepare to find start position {}:{}:{}", entryPosition.getJournalName(), "", entryPosition.getTimestamp());
                   specificLogFilePosition = findAsPerTimestampInSpecificLogFile( entryPosition.getTimestamp(), entryPosition, entryPosition.getJournalName());
               }

               if (specificLogFilePosition == null) {
                   // position不存在，从文件头开始
                   entryPosition.setPosition(BINLOG_START_OFFEST);
                   return entryPosition;
               } else {
                   return specificLogFilePosition;
               }
           }
        } else {
           if (logPosition.getIdentity().getSourceAddress().equals( connector.getAddress() ) ) {
               logger.warn("prepare to find start position {} as last position", logPosition.getPostion());
               return logPosition.getPostion();
           } else {
               // 针对切换的情况，考虑回退时间
               long newStartTimestamp = logPosition.getPostion().getTimestamp() - fallbackIntervalInSeconds * 1000;
               logger.warn("prepare to find start position by switch {}:{}:{}", "", "",logPosition.getPostion().getTimestamp() );
               return findByStartTimeStamp(newStartTimestamp);
           }
       }
    }
    
    // 根据想要的position，可能这个position对应的记录为rowdata，需要找到事务头，避免丢数据
    // 主要考虑一个事务执行时间可能会几秒种，如果仅仅按照timestamp相同，则可能会丢失事务的前半部分数据
    private Long findTransactionBeginPosition(final EntryPosition entryPosition)throws IOException {
        // 尝试找到一个合适的位置
        final AtomicBoolean reDump = new AtomicBoolean(false);
        final AtomicReference<ReplicationPosition> lastPosition = new AtomicReference<>();
        reconnect();
        seek(entryPosition.getJournalName(), entryPosition.getPosition(), new Sink() {
            public boolean sink(LogEvent event) {
                try {
                    CanalEntry.Entry entry = binlogParser.parse(event);
                    if (entry == null) {
                        return true;
                    }

                    // 直接查询第一条业务数据，确认是否为事务Begin/End
                    if (CanalEntry.EntryType.TRANSACTIONBEGIN == entry.getEntryType()
                        || CanalEntry.EntryType.TRANSACTIONEND == entry.getEntryType()) {
                        lastPosition.set( buildLastPosition(entry) );
                        return false;
                    } else {
                        reDump.set(true);
                        return false;
                    }
                } catch (Exception e) {
                    // 上一次记录的poistion可能为一条update/insert/delete变更事件，直接进行dump的话，会缺少tableMap事件，导致tableId未进行解析
                    processError(e, lastPosition.get(), entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                }
            }
        });     
        if( lastPosition.get()!=null )
        	return lastPosition.get().getPostion().getPosition();
   
        // 针对开始的第一条为非Begin记录，需要从该binlog扫描
        if (reDump.get()) {
            final AtomicLong preTransactionStartPosition = new AtomicLong(0L);
            reconnect();
            
            // 直接查询第一条业务数据，确认是否为事务Begin
            // 记录一下transaction begin position
            seek(entryPosition.getJournalName(), 4L, new Sink() {
                public boolean sink(LogEvent event) {
                    try {
                        CanalEntry.Entry entry = binlogParser.parse(event);
                        if (entry != null) {
                            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                                && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                                preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());
                            }
                            if (entry.getHeader().getLogfileOffset() >= entryPosition.getPosition() ) {
                                return false;// 退出
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        logger.error( "error on find next transaction start position", e);
                        return false;
                    }
                }
            });

            // 判断一下找到的最接近position的事务头的位置
            if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
                logger.error("IMPORT!!!preTransactionEndPosition greater than startPosition , maybe lost data");
            }
            if( preTransactionStartPosition.get() >= entryPosition.getPosition() )
            	return preTransactionStartPosition.get();
        }
        return entryPosition.getPosition();
    }
    /**
     * 查询当前的binlog位置
     */
    private EntryPosition findStartPosition2() {
        try {
            ResultSetPacket packet = queryExecutor.query("show binlog events limit 1");
            List<String> fields = packet.getFieldValues();
            if ( fields==null || fields.isEmpty() ) {
                throw new ParseException("command : 'show binlog events limit 1' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            throw new ParseException("command : 'show binlog events limit 1' has an error!", e);
        }
    }
    
	private EntryPosition findEndPosition() {
        try {
            ResultSetPacket packet = queryExecutor.query("show master status");
            List<String> fields = packet.getFieldValues();
            if (fields==null || fields.isEmpty()) {
                throw new ParseException("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            throw new ParseException("command : 'show master status' has an error!", e);
        }
    }
		
    /**
     * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
     * 针对最后一个binlog会给定endPosition，避免无尽的查询
     */
    private EntryPosition findAsPerTimestampInSpecificLogFile(final Long startTimestamp,
                                                              final EntryPosition endPosition,
                                                              final String searchBinlogFile) {
        final ReplicationPosition logPosition = new ReplicationPosition();
        try {
            reconnect();
            // 开始遍历文件
            seek(searchBinlogFile, 4L, new Sink() {
                private ReplicationPosition lastPosition;
                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        CanalEntry.Entry entry = binlogParser.parse(event);
                        if (entry == null) {
                            return true;
                        }
                        String logfilename = entry.getHeader().getLogfileName();
                        Long logfileoffset = entry.getHeader().getLogfileOffset();
                        Long logposTimestamp = entry.getHeader().getExecuteTime();

                        if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())
                            || CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            logger.debug("compare exit condition:{},{},{}, startTimestamp={}...", logfilename, logfileoffset, logposTimestamp, startTimestamp );
                            // 事务头和尾寻找第一条记录时间戳，如果最小的一条记录都不满足条件，可直接退出
                            if (logposTimestamp >= startTimestamp) {
                                return false;
                            }
                        }

                        if (StringUtils.equals(endPosition.getJournalName(), logfilename)
                            && endPosition.getPosition() <= (logfileoffset + event.getEventLen())) {
                            return false;
                        }

                        // 记录一下上一个事务结束的位置，即下一个事务的position
                        // position = current +
                        // data.length，代表该事务的下一条offest，避免多余的事务重复
                        if (CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename, logfileoffset + event.getEventLen(), logposTimestamp);
                            logger.debug("set {} to be pending start position before finding another proper one...", entryPosition);
                            logPosition.setPostion(entryPosition);
                        } else if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())) {
                            // 当前事务开始位点
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp);
                            logger.debug("set {} to be pending start position before finding another proper one...", entryPosition);
                            logPosition.setPostion(entryPosition);
                        }
                    } catch (Exception e) {
                        processError(e, lastPosition, searchBinlogFile, 4L);
                    }
                   	return false;
                }
            });
        } catch (IOException e) {
            logger.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e);
        }

        if (logPosition.getPostion() != null) {
            return logPosition.getPostion();
        } else {
            return null;
        }
    }
	    
    private ReplicationPosition buildLastPosition(CanalEntry.Entry entry) { // 初始化一下
    	ReplicationPosition logPosition = new ReplicationPosition();
        
    	EntryPosition position = new EntryPosition();
        position.setJournalName(entry.getHeader().getLogfileName() );
        position.setPosition( entry.getHeader().getLogfileOffset() );
        position.setTimestamp( entry.getHeader().getExecuteTime() );
        logPosition.setPostion(position);

        SlaveIdentity identity = new SlaveIdentity(connector.getAddress(), -1L);
        logPosition.setIdentity(identity);
        return logPosition;
    }
	    
    private void processError(Exception e, ReplicationPosition lastPosition, String startBinlogFile, long startPosition) {
        if (lastPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]",
                lastPosition.getPostion()),
                e);
        } else {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s,%s]",
                startBinlogFile,
                startPosition), e);
        }
    }
	    
    // 根据时间查找binlog位置
    private EntryPosition findByStartTimeStamp(Long startTimestamp) {
        EntryPosition endPosition = findEndPosition();
        EntryPosition startPosition = findStartPosition2();
        String maxBinlogFileName = endPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();
        logger.info("show master status to set search end condition:{} ", endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();
        boolean shouldBreak = false;
        while(!shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(startTimestamp, endPosition, startSearchBinlogFile);
                if (entryPosition == null) {
                    if (StringUtils.equalsIgnoreCase(minBinlogFileName, startSearchBinlogFile)) {
                        // 已经找到最早的一个binlog，没必要往前找了
                        shouldBreak = true;
                        logger.warn("Didn't find the corresponding binlog files from {} to {}",
                            minBinlogFileName,
                            maxBinlogFileName);
                    } else {
                        // 继续往前找
                        int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                        if (binlogSeqNum <= 1) {
                            logger.warn("Didn't find the corresponding binlog files");
                            shouldBreak = true;
                        } else {
                            int nextBinlogSeqNum = binlogSeqNum - 1;
                            String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                                startSearchBinlogFile.indexOf(".") + 1);
                            String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                            startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                        }
                    }
                } else {
                    logger.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                logger.error("the binlogfile:"+startSearchBinlogFile+" doesn't exist, to continue to search the next binlogfile", e);
                int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    logger.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                        startSearchBinlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }
        }
        // 找不到
        return null;
	}
	    
    private void sendBinlogDump(String binlogfilename, Long binlogPosition) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[] { ByteBuffer.wrap(binlogDumpHeader.toBytes()),
                ByteBuffer.wrap(cmdBody) });

        connector.setDumping(true);
    }

    public MySQLPumper fork() {
        MySQLPumper connection = new MySQLPumper();
        connection.setSlaveId(getSlaveId());
        connection.setConnector(connector.fork());
        return connection;
    }
    
    private void updateSettings() throws IOException {
        try {
            queryExecutor.update("set wait_timeout=9999999");
        } catch (Exception e) {
            logger.error("error on set mysql connection paramter", e);
        }
        try {
        	queryExecutor.update("set net_write_timeout=1800");
        } catch (Exception e) {
        	logger.error("error on set mysql connection paramter", e);
        }

        try {
        	queryExecutor.update("set net_read_timeout=1800");
        } catch (Exception e) {
        	logger.error("error on set mysql connection paramter", e);
        }

        try {
            // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
        	queryExecutor.update("set names 'binary'");
        } catch (Exception e) {
        	logger.error("error on set mysql connection paramter", e);
        }

        try {
            // mysql5.6针对checksum支持需要设置session变量
            // 如果不设置会出现错误： Slave can not handle replication events with the
            // checksum that master is configured to log
            // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
        	queryExecutor.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
        } catch (Exception e) {
        	logger.error("error on set mysql connection paramter", e);
        }

        try {
            // mariadb针对特殊的类型，需要设置session变量
        	queryExecutor.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        } catch (Exception e) {
        	logger.error("error on set mysql connection paramter", e);
        }
    }
}
