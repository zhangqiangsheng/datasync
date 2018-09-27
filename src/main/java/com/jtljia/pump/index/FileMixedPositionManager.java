package com.jtljia.pump.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jtljia.pump.canal.protocol.position.ReplicationPosition;
import com.jtljia.pump.JsonUtils;
import com.jtljia.pump.exception.PumpException;

/**
 * 基于文件刷新的log position实现
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)
 * </pre>
 */
public class FileMixedPositionManager extends MemoryPositionManager {

    private static final Logger      logger       =  LoggerFactory.getLogger(FileMixedPositionManager.class);
    private static final Charset     charset      =  Charset.forName("UTF-8");
    private File                     dataDir;
    private Map<String, File>        dataFileCaches;
    private ScheduledExecutorService executor;

    private long                     period       = 1000;                                                      // 单位ms
    private volatile Set<String>     persistTasks = new HashSet<String>();
    
    public void setDataDir(String dataDir) {
        this.dataDir = new File(dataDir);
    }

    public void setDataDir(File dataDir) {
        this.dataDir = dataDir;
    }

    public void setPeriod(long period) {
        this.period = period;
    }
    
    public void start() {
        super.start();
        if (!dataDir.exists()) {
            try {
                FileUtils.forceMkdir(dataDir);
            } catch (IOException e) {
                throw new PumpException(e);
            }
        }

        if (!dataDir.canRead() || !dataDir.canWrite()) {
            throw new PumpException("dir[" + dataDir.getPath() + "] can not read/write");
        }

        dataFileCaches = new ConcurrentHashMap<String, File>();

        executor = Executors.newScheduledThreadPool(1);

        // 启动定时工作任务
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
            	Set<String> newTaskSet = new HashSet<>();
            	Set<String> oldTaskSet = persistTasks;
            	persistTasks = newTaskSet;
            	oldTaskSet.forEach( destination->{
            		try {
                        // 定时将内存中的最新值刷到file中，多次变更只刷一次
                        flushDataToFile(destination);
                    } catch (Throwable e) {
                        logger.error("period update" + destination + " curosr failed!", e);
                    }
            	});
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void doStop() {
        flushDataToFile();
        executor.shutdownNow();
        positions.clear();
    }

    public void persistLogPosition(String destination, ReplicationPosition logPosition) {
        logger.info( "move sinked postion to:{}-{}-{}", 
        		logPosition.getPostion().getJournalName(), 
        		logPosition.getPostion().getPosition(), 
        		DateFormatUtils.format( new Date(logPosition.getPostion().getTimestamp()), "yyyy-MM-dd HH:mm:ss SSS") );
    	persistTasks.add(destination);// 添加到任务队列中进行触发
        super.persistLogPosition(destination, logPosition);
    }

    @Override
    public ReplicationPosition getLatestIndexBy(String destination) {
    	ReplicationPosition result = super.getLatestIndexBy( destination );
    	if( result==null ) 
    		result = loadDataFromFile( new File(dataDir, destination) );
    	return result;
    }

    private void flushDataToFile() {
        for (String destination : positions.keySet()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
    	File f = dataFileCaches.get(destination);
    	if( f==null ) {
    		f = new File(dataDir, destination);
    		dataFileCaches.put( destination, f );
    	}
    	
    	ReplicationPosition position = positions.get(destination);
        if (position != null) {
            String json = JsonUtils.marshalToString(position);
            try {
                FileUtils.writeStringToFile(f, json);
            } catch (IOException e) {
                throw new PumpException(e);
            }
        }
    }

    private ReplicationPosition loadDataFromFile(File dataFile) {
        try {
            if( dataFile.exists() ) {
            	String json = FileUtils.readFileToString(dataFile, charset.name());
                if( json!=null && !json.isEmpty() )
                	return JsonUtils.unmarshalFromString(json, ReplicationPosition.class);
            }
            return null;
        } catch (IOException e) {
            throw new PumpException(e);
        }
    }
}
