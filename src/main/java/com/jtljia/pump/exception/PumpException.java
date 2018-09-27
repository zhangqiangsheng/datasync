package com.jtljia.pump.exception;

/**
 * @author felix.wu
 * @version 1.0.0
 */
public class PumpException extends RuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public PumpException(String errorCode){
        super(errorCode);
    }

    public PumpException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public PumpException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public PumpException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public PumpException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
