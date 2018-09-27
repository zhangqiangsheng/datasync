package com.jtljia.pump;

/**
 * 服务生命周期，包括状态转换操作
 * @author felix.wu
 * @version 1.0.0
 */
public interface ServiceLifeCycle {
	public static enum STATE{
		INIT, STARTING, RUNNING, STOPPING, STOPPED 
	}
	
    void start();

    void stop();
    
    boolean isRunning();
    
    boolean isShouldShutdown();
    
    boolean isShouldWork();
    
    STATE getCurrentState();
}
