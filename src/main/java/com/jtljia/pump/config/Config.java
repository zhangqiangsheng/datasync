package com.jtljia.pump.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wujie
 */
public class Config {
	private static Logger logger = LoggerFactory.getLogger( Config.class );
	
	private Properties properties = new Properties();
	
	public void load(Properties p){
		p.stringPropertyNames().forEach( name->{
			properties.setProperty(name, p.getProperty(name));
		});
	}
	
	public Optional<String> get(String key){
		String value = properties.getProperty(key);
		return Optional.ofNullable( StringUtils.isBlank(value)?null:value.trim() );
	}
	
	public Optional<Integer> getInt(String key){
		Optional<String> v = get(key);
		return v.isPresent()?Optional.of( Integer.valueOf( v.get()) ):Optional.empty() ;
	}
	
	public void load(String url){
		InputStream is = null;
		try{
			if( url.startsWith("classpath:") ) {
				url = url.substring( "classpath:".length() );
				is = this.getClass().getResourceAsStream(url);
			} else {
				is = new URL(url).openStream();
			}
			properties.load(is);
		}catch (Exception e) {
			logger.error( "error on load config", e);
		} finally {
			if( is!=null )
				try {
					is.close();
				} catch (IOException e) {
					logger.error( "error on close config input stream", e);
				}
		}
	}
	
	public void dump(){
		logger.info("**********current config:**********");
		properties.entrySet().forEach( e->{
			logger.info( "{}={}", e.getKey(), e.getValue() );
		});
		logger.info("***********************************");
	}
	
	public static class factory{
		private static Config instance=null;
		
		public static Config getInstance(){
			if( instance==null ){
				instance = new Config();
				String configPath = System.getProperties().getProperty("config");
				if( configPath!=null ){
					instance.load( configPath.trim() );
				} 
				instance.load( System.getProperties() );
				instance.dump();
			}
			return instance;
		}
	}
}
