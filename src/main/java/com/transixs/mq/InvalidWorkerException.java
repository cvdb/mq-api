package com.transixs.mq;

public class InvalidWorkerException extends RuntimeException {

    public InvalidWorkerException(String message) {
        super(message);
    }

    public InvalidWorkerException(String message, Throwable cause) {
        super(message, cause);
    }
}
