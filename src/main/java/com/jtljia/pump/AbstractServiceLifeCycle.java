package com.jtljia.pump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jtljia.pump.exception.PumpException;
/**
 * 基本实现
 * @author felix.wu
 * @version 1.0.0
 */
public abstract class AbstractServiceLifeCycle implements ServiceLifeCycle {
	private static Logger logger = LoggerFactory.getLogger( AbstractServiceLifeCycle.class );
    private volatile STATE state = STATE.INIT; // 是否处于运行中
    
    public STATE getCurrentState(){
    	return state;
    }
        
    public boolean isRunning() {
        return state==STATE.RUNNING;
    }
    
    public boolean isShouldShutdown(){
    	return state==STATE.STOPPING || state==STATE.STOPPED;
    }
    
    public boolean isShouldWork(){
    	return state==STATE.STARTING || state==STATE.RUNNING;
    }
    
    public void start() {
        if( state!=STATE.INIT )
        	throw new PumpException(this.getClass().getName() + " is " +state.name() + ", can't start");
        state = STATE.STARTING;
        logger.info( this.getClass().getName() + " starting");
        
        doStart();
        
        if( state != STATE.STARTING ) 
        	throw new PumpException(this.getClass().getName() + " is " +state.name() + ", can't start done");
	   	state = STATE.RUNNING;
	   	logger.info( this.getClass().getName() + " started, running");
    }
    
    protected abstract void doStart();
    
    public void stop() {
        if( state != STATE.RUNNING )
            throw new PumpException(this.getClass().getName() + " is " +state.name() + ", can't stop");
    	state = STATE.STOPPING;
    	logger.info( this.getClass().getName() + " stopping");
    	
    	doStop();
    	
    	if( state != STATE.STOPPING )
            throw new PumpException(this.getClass().getName() + " is " +state.name() + ", can't stop done");
    	state = STATE.STOPPED;
    	logger.info( this.getClass().getName() + " stopped");
    }
    
    protected abstract void doStop(); 
}