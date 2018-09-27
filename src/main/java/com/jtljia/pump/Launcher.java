package com.jtljia.pump;

public class Launcher {
	
	public static void main(String[] args) {
		MySQLPumperService service = new MySQLPumperService();
		service.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                	service.stop();
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("## pumper service is down.");
                }
            }
        });
	}
}
