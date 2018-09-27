package com.jtljia.pump;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.jtljia.pump.config.Config;
import com.jtljia.pump.util.HttpClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tddl.dbsync.binlog.BinlogParser;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.MySQLBinLogParser;
import com.taobao.tddl.dbsync.binlog.TableMetaCache;
import com.taobao.tddl.dbsync.binlog.TableNotFoundException;
import com.jtljia.pump.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.jtljia.pump.config.ConfigParameter;
import com.jtljia.pump.exception.ParseException;
import com.jtljia.pump.exception.PumpException;
import com.jtljia.pump.index.FileMixedPositionManager;
import com.jtljia.pump.index.ReplicationPositionManager;
import com.jtljia.pump.pusher.TablePusher;
import com.jtljia.pump.pusher.ToMySQLPusher;

/**
 * @author wujie
 */
public class MySQLPumperService extends AbstractServiceLifeCycle{

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private ConfigParameter cp = new ConfigParameter();
    
    //源数据库实例信息
    private AuthenticationInfo instanceInfo;        // 主库
    
    // binLogParser
    private BinlogParser<LogEvent>    binlogParser = null;
    private Thread                    pumperThread   = null;

    protected long            lastEntryTime              = 0L;
    private volatile Timer    timer;
    private MysqlDetectingTimeTask heartBeatTimerTask;
    
    private ReplicationPositionManager positionManager;
    
    private ToMySQLPusher pusher;
    // 心跳检查信息
    private MysqlQueryExecutor metaQueryExecutor;       // 查询meta信息的链接
    private TableMetaCache     tableMetaCache;          // 对应meta
    private BinlogFormat[]     supportBinlogFormats;    // 支持的binlogFormat,如果设置会执行强校验
    private BinlogImage[]      supportBinlogImages;     // 支持的binlogImage,如果设置会执行强校验
    private BinlogFormat       binlogFormat;
    private BinlogImage        binlogImage;
    
    private boolean            tableNotFoundException = false;
    
    public MySQLPumperService(){
    	binlogParser = buildParser();// 初始化一下BinLogParser
    	
    	InetSocketAddress address = new InetSocketAddress( cp.instanceIP, cp.instancePort );
    	instanceInfo = new AuthenticationInfo( address, cp.instanceUsername, cp.instancePassword );
    	
    	positionManager = new FileMixedPositionManager();
    	((FileMixedPositionManager)positionManager).setDataDir( cp.positionManagerDir );
    }

    public void doStart() {
    	positionManager.start();
        // 启动工作线程
    	pumperThread = new Thread(new Runnable() {
            public void run() {
            	MySQLPumper pumper  = null;
                while(isShouldWork()) {
                    try {
                        // 1. 构造抽水器
                    	pumper = buildPumper();
                        // 2. 启动一个心跳线程
                        startHeartBeat(pumper);
                        // 3. 执行dump前的准备工作
                        preDump(pumper);
                        // 4. 开始dump数据
                        pumper.dump();
                    } catch (TableNotFoundException e) {
                    	tableNotFoundException = true;
                    	logger.error( "table not found", e );
                    } catch (SQLException e) {
                    	throw new PumpException("sql error interrupt pumper", e);
                    } catch (IOException e) {
                        logger.error(String.format("dump address %s has an error, retrying. caused by ", instanceInfo.getAddress().toString()), e);
                    } finally {
                        // 关闭一下链接
                        afterDump();
                        try {
                        	pumper.disconnect();
                        } catch (IOException e1) {
                            logger.error("disconnect address {} has an error, retrying., caused by ", instanceInfo.getAddress().toString(), e1);
                        }
                    }
                    binlogParser.reset();// 重新置位
                    if (isShouldWork()) {
                        // sleep一段时间再进行重试
                        try{ Thread.sleep(20000); } catch (InterruptedException e) {}
                    }
                }
            }
        });

    	pumperThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t,Throwable e) {
                logger.error("uncatch exception in pumper thread",e);
                stop();
                //服务异常退出钉钉提示
                try {
                    sendDingMessage("pump服务异常导致崩溃，IP地址为："+InetAddress.getLocalHost().getHostAddress()+"请查看原因");
                } catch (UnknownHostException e1) {
                }
            }
        });
    	pumperThread.setName(String.format("Pumper:address = %s", instanceInfo.getAddress().toString()));
    	pumperThread.start();
    }

    public void sendDingMessage(String msg){
        String url = Config.factory.getInstance().get("ding.msg.url").get();
        Map<String,Object> body = new HashMap<>();
        body.put("msgtype","text");
        Map<String,Object> content = new HashMap<>();
        content.put("content",msg);
        body.put("text",content);
        Map<String,Object> at = new HashMap<>();
        at.put("isAtAll",false);
        List<String> mobiles = new ArrayList<>();
        mobiles.add("15051463570");
        at.put("atMobiles",mobiles);
        body.put("at",at);
        Map<String,String> header = new HashMap<>();
        header.put("Content-Type","application/json");
        HttpClientUtil.HttpPost(url,header,body);
    }
    
    private void startHeartBeat(MySQLPumper pumper) {
        lastEntryTime = 0L; // 初始化
        if (timer == null) {// lazy初始化一下
            String name = String.format("HeartBeatTimeTask:address = %s", instanceInfo.getAddress().toString());
            synchronized (MySQLPumper.class) {
                if (timer == null) {
                    timer = new Timer(name, true);
                }
            }
        }

        if (heartBeatTimerTask == null) {// 避免重复创建heartbeat线程
            heartBeatTimerTask = new MysqlDetectingTimeTask( pumper.getQueryExecutor().fork() );
            timer.schedule(heartBeatTimerTask, cp.detectingIntervalInSeconds * 1000L, cp.detectingIntervalInSeconds * 1000L);
            logger.info("start heart beat.... ");
        }
    }

    protected void stopHeartBeat() {
        lastEntryTime = 0L; // 初始化
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        if (heartBeatTimerTask != null) {
        	heartBeatTimerTask.destory();
            heartBeatTimerTask = null;
        }
    }
   
    private MySQLBinLogParser buildParser() {
        MySQLBinLogParser convert = new MySQLBinLogParser();
        convert.setCharset( Charset.forName( cp.connectionCharset ) );
        convert.setFilterQueryDcl( cp.filterQueryDcl );
        convert.setFilterQueryDml( cp.filterQueryDml );
        convert.setFilterQueryDdl( cp.filterQueryDdl );
        convert.setFilterRows( cp.filterRows );
        convert.setFilterTableError( cp.filterTableError );
        return convert;
    }
    
    private void preDump(MySQLPumper pumper) {
        if (binlogParser != null && binlogParser instanceof MySQLBinLogParser) {
        	metaQueryExecutor = pumper.getQueryExecutor().fork();
            try {
            	metaQueryExecutor.connect();
            } catch (IOException e) {
                throw new ParseException(e);
            }

            if (supportBinlogFormats != null && supportBinlogFormats.length > 0) {
                BinlogFormat format = getBinlogFormat();
                boolean found = false;
                for (BinlogFormat supportFormat : supportBinlogFormats) {
                    if (supportFormat != null && format == supportFormat) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new ParseException("Unsupported BinlogFormat " + format);
                }
            }

            if (supportBinlogImages != null && supportBinlogImages.length > 0) {
                BinlogImage image = getBinlogImage();
                boolean found = false;
                for (BinlogImage supportImage : supportBinlogImages) {
                    if (supportImage != null && image == supportImage) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new ParseException("Unsupported BinlogImage " + image);
                }
            }

            tableMetaCache = new TableMetaCache(metaQueryExecutor);
            ((MySQLBinLogParser) binlogParser).setTableMetaCache(tableMetaCache);
        }
    }

    private void afterDump() {
        if (metaQueryExecutor != null) {
            try {
            	metaQueryExecutor.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection", e);
            }
        }
    }
    
	public void doStop() throws ParseException {
        if (metaQueryExecutor != null) {
            try {
            	metaQueryExecutor.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection", e);
            }
        }

        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }

        stopHeartBeat(); // 先停止心跳
        
        pumperThread.interrupt(); // 尝试中断
        try {
        	pumperThread.join();// 等待其结束
        } catch (InterruptedException e) {
            // ignore
        }
        positionManager.stop();
        pusher.close();
    }
	
    public BinlogFormat getBinlogFormat() {
        if (binlogFormat == null) {
            synchronized (this) {
                loadBinlogFormat();
            }
        }

        return binlogFormat;
    }

    public BinlogImage getBinlogImage() {
        if (binlogImage == null) {
            synchronized (this) {
                loadBinlogImage();
            }
        }

        return binlogImage;
    }
    
    /**
     * 获取一下binlog format格式
     */
    private void loadBinlogFormat() {
        ResultSetPacket rs = null;
        try {
            rs = metaQueryExecutor.query("show variables like 'binlog_format'");
        } catch (IOException e) {
            throw new ParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }

        binlogFormat = BinlogFormat.valuesOf(columnValues.get(1));
        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    /**
     * 获取一下binlog image格式
     */
    private void loadBinlogImage() {
        ResultSetPacket rs = null;
        try {
            rs = metaQueryExecutor.query("show variables like 'binlog_row_image'");
        } catch (IOException e) {
            throw new ParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            // 可能历时版本没有image特性
            binlogImage = BinlogImage.FULL;
        } else {
            binlogImage = BinlogImage.valuesOf(columnValues.get(1));
        }

        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog image query result:" + rs.getFieldValues());
        }
    }
    /**
     * 心跳信息
     */
    class MysqlDetectingTimeTask extends TimerTask {

        private boolean         reconnect = false;
        private MysqlQueryExecutor queryExecutor;

        public MysqlDetectingTimeTask(MysqlQueryExecutor queryExecutor){
            this.queryExecutor = queryExecutor;
        }
        
        public void destory() {
            try {
            	queryExecutor.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect heartbeat connection",  e);
            }
        }
        
        public void run() {
            try {
                if (reconnect) {
                    reconnect = false;
                    queryExecutor.reconnect();
                } else if (!queryExecutor.isConnected()) {
                	queryExecutor.connect();
                }
                // 可能心跳sql为select 1
                if ( StringUtils.startsWithIgnoreCase(cp.detectingSQL, "select")
                  || StringUtils.startsWithIgnoreCase(cp.detectingSQL, "show")
                  || StringUtils.startsWithIgnoreCase(cp.detectingSQL, "explain")
                  || StringUtils.startsWithIgnoreCase(cp.detectingSQL, "desc")) {
                	queryExecutor.query(cp.detectingSQL);
                } else {
                	queryExecutor.update(cp.detectingSQL);
                }
            } catch (SocketTimeoutException e) {
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            } catch (IOException e) {
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            } catch (Throwable e) {
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            }
        }

        public MysqlQueryExecutor getMysqlConnection() {
            return queryExecutor;
        }
    }

    private MySQLPumper buildPumper() throws SQLException {
    	MySQLPumper pumper = new MySQLPumper( instanceInfo.getAddress(), instanceInfo.getUsername(), instanceInfo.getPassword(),
    											  cp.connectionCharsetNumber, instanceInfo.getDefaultDatabaseName());
    	pumper.getConnector().setReceiveBufferSize(cp.receiveBufferSize);
    	pumper.getConnector().setSendBufferSize(cp.sendBufferSize);
    	pumper.getConnector().setSoTimeout( cp.defaultConnectionTimeoutInSeconds * 1000);
    	pumper.setSlaveId( cp.slaveId);
    	pumper.setNeedTransactionPosition( tableNotFoundException );
    	tableNotFoundException = false;
    	
    	InetSocketAddress targetAddress = new InetSocketAddress( cp.targetInstanceIp, cp.targetInstancePort );
    	AuthenticationInfo targetInfo = new AuthenticationInfo( targetAddress, cp.targetInstanceUsername, cp.targetInstancePassword );
    	
    	List<TablePusher> tpList = buildTablePusher();
    	
    	pusher = new ToMySQLPusher( targetInfo );
    	tpList.forEach( tp-> tp.setParent( pusher) );
    	pusher.setFilterTables( cp.filterTables );
    	pusher.setTablePusherList( tpList );
    	
    	pumper.setBinlogParser( binlogParser );
    	pumper.setPusher( pusher );
    	pumper.setPositionManager( positionManager );
    	
        return pumper;
    }
    
    private List<TablePusher> buildTablePusher(){
    	List<TablePusher> result = new ArrayList<>();
    	
    	TablePusher tp1 = new TablePusher();
    	tp1.setTargetTableName( cp.targetInstanceDb+".syn_lms_saleclues" );
    	tp1.setTableName( cp.instanceDb+".saleclues" );
    	tp1.getColumnMap().add( new Tuple4<>("id","id",true, null) );
    	tp1.getColumnMap().add( new Tuple4<>("house_id","house_id",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("source_cd","source_cd",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("status_cd","status",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("apply_status_cd","apply_status_cd",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("org_id","org_id",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("create_dt","create_time",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("create_by","create_by",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("channel_no","channel_no",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("channel_name","channel_name",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("channel_followby","channel_followby",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("app_source_cd","app_source_cd",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("source_page","source_page",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("share_user_id","share_user_id",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("sub_code_back_up","sub_code_back_up",false, null) );
    	tp1.getColumnMap().add( new Tuple4<>("lms_type","type",false, null) );
        tp1.getColumnMap().add( new Tuple4<>("receipt_way","share_user_way",false, null) );
        tp1.getColumnMap().add( new Tuple4<>("assign_org_date","assign_org_time",false, null) );
        tp1.getColumnMap().add( new Tuple4<>("receipt_way_confirm","receipt_way_confirm",false, null) );
        tp1.getColumnMap().add( new Tuple4<>("call_label","call_label",false, null) );
    	result.add( tp1 );
    	
    	TablePusher tp2 = new TablePusher();
    	tp2.setTargetTableName( cp.targetInstanceDb+".syn_lms_saleclues" );
    	tp2.setTableName( cp.instanceDb+".saleclues_assign_member" );
    	tp2.getColumnMap().add( new Tuple4<>("id","saleclues_id",true, null) );
    	tp2.getColumnMap().add( new Tuple4<>("customer_manager","customer_manager",false, null) );
    	tp2.getColumnMap().add( new Tuple4<>("customer_director_id","customer_director_id",false, null) );
    	tp2.getColumnMap().add( new Tuple4<>("customer_manager_id", "customer_manager_id",false, null) );
    	tp2.getColumnMap().add( new Tuple4<>("designer_director_id","designer_director_id",false, null) );
    	tp2.getColumnMap().add( new Tuple4<>("designer_id","designer_id",false, null) );
    	tp2.getColumnMap().add( new Tuple4<>("designer","designer",false, null) );
        tp2.getColumnMap().add( new Tuple4<>("business_manager_id","business_manager_id",false, null) );
        tp2.getColumnMap().add( new Tuple4<>("business_manager","business_manager",false, null) );
        tp2.getColumnMap().add( new Tuple4<>("assign_date","manager_assign_time",false, null) );
    	result.add( tp2 );
    	
    	TablePusher tp3 = new TablePusher();
    	tp3.setTargetTableName( cp.targetInstanceDb+".syn_lms_saleclues" );
    	tp3.setTableName( cp.instanceDb+".saleclues_assist" );
    	tp3.getColumnMap().add( new Tuple4<>("id","sid",true, null) );
    	tp3.getColumnMap().add( new Tuple4<>("arrival_time","arrival_time",false, null) );
    	result.add( tp3 );

        TablePusher tp7 = new TablePusher();
        tp7.setTargetTableName( cp.targetInstanceDb+".syn_lms_saleclues" );
        tp7.setTableName( cp.instanceDb+".saleclues_intention" );
        tp7.getColumnMap().add( new Tuple4<>("id","saleclues_id",true, null) );
        tp7.getColumnMap().add( new Tuple4<>("design_style_cd","design_style_cd",false, null) );
        result.add( tp7 );

        TablePusher tp4 = new TablePusher();
        tp4.setTargetTableName( cp.targetInstanceDb+".syn_lms_house" );
        tp4.setTableName( cp.instanceDb+".house" );
        tp4.getColumnMap().add( new Tuple4<>("id", "id",true, null) );
        tp4.getColumnMap().add( new Tuple4<>("customer_id","customer_id",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("region_id", null, false, sourceColumns->{
            return (String)sourceColumns.stream().filter( sc-> ( sc.getName().equals( "county_id") || sc.getName().equals( "city_id") || sc.getName().equals( "province_id") ) && !sc.getValue().isEmpty() )
                                                  .map( sc-> {
                                                      Integer scope = 0;
                                                      switch ( sc.getName() ) {
                                                          case  "county_id":    scope = 1; break;
                                                          case  "city_id":      scope = 2; break;
                                                          case  "province_id": scope = 3;
                                                      }
                                                      return new Object[]{scope, sc.getValue()};
                                                  })
                                                  .reduce( (p1,p2)-> {
                                                      if( (Integer)p1[0]<(Integer)p2[0])
                                                          return p1;
                                                      else
                                                          return p2;
                                                  } ).orElse( new Object[]{null, null} )[1];
        }) );
        tp4.getColumnMap().add( new Tuple4<>("community_name","community_name",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("house_number","house_number",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("area","area",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("struct_room","struct_room",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("struct_hall","struct_hall",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("struct_kitchen","struct_kitchen",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("struct_bathroom","struct_bathroom",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("struct_balcony","struct_balcony",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("house_address","address",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("actual_area","actual_area",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("toilet","struct_bathroom",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("ground_area","area",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("area_delta","area",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("create_dt","create_dt",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("create_by","create_by",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("is_deliveryhouse","is_deliveryhouse",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("house_classify","house_classify",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("house_type_cd","house_type_cd",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("deliveryhouse_date","deliveryhouse_date",false, null) );
        tp4.getColumnMap().add( new Tuple4<>("is_county","is_county",false, null) );
        result.add( tp4 );

        TablePusher tp5 = new TablePusher();
        tp5.setTargetTableName( cp.targetInstanceDb+".syn_common_customer" );
        tp5.setTableName( cp.instanceDb+".customer" );
        tp5.getColumnMap().add( new Tuple4<>("id",           "id",true, null) );
        tp5.getColumnMap().add( new Tuple4<>("name",          "name",false, null) );
        tp5.getColumnMap().add( new Tuple4<>("mobile",        "mobile",false, null) );
        tp5.getColumnMap().add( new Tuple4<>("create_dt",      "create_time",false, null) );
        tp5.getColumnMap().add( new Tuple4<>("idcard_no",       "idcard_no",false, null) );
        tp5.getColumnMap().add( new Tuple4<>("idcard_no_type",  "idcard_no_type",false, null) );
        result.add( tp5 );

        TablePusher tp6 = new TablePusher();
        tp6.setTargetTableName( cp.targetInstanceDb+".syn_common_customer" );
        tp6.setTableName( cp.instanceDb+".customer_info" );
        tp6.getColumnMap().add( new Tuple4<>("id","customer_id",true, null) );
        tp6.getColumnMap().add( new Tuple4<>("is_vip","is_vip",false, null) );
        tp6.getColumnMap().add( new Tuple4<>("source_cd","source_cd",false, null) );
        tp6.getColumnMap().add( new Tuple4<>("app_source_cd","app_source_cd",false, null) );
        tp6.getColumnMap().add( new Tuple4<>("channel","channel",false, null) );
        tp6.getColumnMap().add( new Tuple4<>("vip_remark","vip_remark",false, null) );
        result.add( tp6 );

    	return result;
    }
    
    /**
     * 查询当前的slave视图的binlog位置
     */
    private SlaveEntryPosition findSlavePosition(MySQLPumper mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.getQueryExecutor().query("show slave status");
            List<FieldPacket> names = packet.getFieldDescriptors();
            List<String> fields = packet.getFieldValues();
            if (fields==null || fields.isEmpty()) {
                return null;
            }

            int i = 0;
            Map<String, String> maps = new HashMap<String, String>(names.size(), 1f);
            for (FieldPacket name : names) {
                maps.put(name.getName(), fields.get(i));
                i++;
            }

            String errno = maps.get("Last_Errno");
            String slaveIORunning = maps.get("Slave_IO_Running"); // Slave_SQL_Running
            String slaveSQLRunning = maps.get("Slave_SQL_Running"); // Slave_SQL_Running
            if ((!"0".equals(errno)) || (!"Yes".equalsIgnoreCase(slaveIORunning))
                || (!"Yes".equalsIgnoreCase(slaveSQLRunning))) {
                logger.warn("Ignoring failed slave: " + mysqlConnection.getConnector().getAddress() + ", Last_Errno = "
                            + errno + ", Slave_IO_Running = " + slaveIORunning + ", Slave_SQL_Running = "
                            + slaveSQLRunning);
                return null;
            }

            String masterHost = maps.get("Master_Host");
            String masterPort = maps.get("Master_Port");
            String binlog = maps.get("Master_Log_File");
            String position = maps.get("Exec_Master_Log_Pos");
            return new SlaveEntryPosition(binlog, Long.valueOf(position), masterHost, masterPort);
        } catch (IOException e) {
            logger.error("find slave position error", e);
        }

        return null;
    }

    public void setSupportBinlogFormats(String formatStrs) {
        String[] formats = StringUtils.split(formatStrs, ',');
        if (formats != null) {
            BinlogFormat[] supportBinlogFormats = new BinlogFormat[formats.length];
            int i = 0;
            for (String format : formats) {
                supportBinlogFormats[i++] = BinlogFormat.valuesOf(format);
            }

            this.supportBinlogFormats = supportBinlogFormats;
        }
    }

    public void setSupportBinlogImages(String imageStrs) {
        String[] images = StringUtils.split(imageStrs, ',');
        if (images != null) {
            BinlogImage[] supportBinlogImages = new BinlogImage[images.length];
            int i = 0;
            for (String image : images) {
                supportBinlogImages[i++] = BinlogImage.valuesOf(image);
            }

            this.supportBinlogImages = supportBinlogImages;
        }
    }
}