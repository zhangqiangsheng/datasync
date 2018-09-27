package com.jtljia.pump.exception;

/**
 * canal 异常定义
 * 
 * @author jianghang 2012-6-15 下午04:57:35
 * @version 1.0.0
 */
public class ParseException extends PumpException {

    private static final long serialVersionUID = -7288830284122672209L;

    public ParseException(String errorCode){
        super(errorCode);
    }

    public ParseException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public ParseException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public ParseException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public ParseException(Throwable cause){
        super(cause);
    }

}
