package com.goel.distributedQueue.interfaces;

public interface Queue {

    public void enqueue(String item) throws Exception;

    public String dequeue() throws Exception;
}
