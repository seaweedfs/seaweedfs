package org.apache.hadoop.seaweed.ftp.controller.vo;

public class Result {

    private boolean status;
    private Object data;
    private String message;

    public Result(boolean status, String message) {
        this.status = status;
        this.message = message;
    }

    public Result(boolean status, Object data, String message) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
