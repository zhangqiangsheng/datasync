package com.taobao.tddl.dbsync.binlog;

public class TableNotFoundException extends RuntimeException {

    private static final long serialVersionUID = -7288830284122672209L;

    public TableNotFoundException(String errorCode){
        super(errorCode);
    }

    public TableNotFoundException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public TableNotFoundException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public TableNotFoundException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public TableNotFoundException(Throwable cause){
        super(cause);
    }

}
