package com.moniepoint.kvstore.pojo;

public class RangeRequest {
    private String startKey;
    private String endKey;

    public RangeRequest() {
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }
}