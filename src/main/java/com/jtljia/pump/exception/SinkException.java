package com.jtljia.pump.exception;

public class SinkException extends PumpException {

    private static final long serialVersionUID = -7288830284122672209L;

    public SinkException(String errorCode){
        super(errorCode);
    }

    public SinkException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public SinkException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public SinkException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public SinkException(Throwable cause){
        super(cause);
    }

}
