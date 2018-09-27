package com.jtljia.pump;

public enum BinlogImage {
	FULL("FULL"), MINIMAL("MINIMAL"), NOBLOB("NOBLOB");

    public boolean isFull() {
        return this == FULL;
    }

    public boolean isMinimal() {
        return this == MINIMAL;
    }

    public boolean isNoBlob() {
        return this == NOBLOB;
    }

    private String value;

    private BinlogImage(String value){
        this.value = value;
    }

    public static BinlogImage valuesOf(String value) {
        BinlogImage[] formats = values();
        for (BinlogImage format : formats) {
            if (format.value.equalsIgnoreCase(value)) {
                return format;
            }
        }
        return null;
    }
}
