package com.migu.schedule.domain;

public class Task {
    private int nodeId;

    private int taskId;

    private int consumption;

    public Task(int taskId, int consumption, int nodeId) {
        this.taskId = taskId;
        this.consumption = consumption;
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getConsumption() {
        return consumption;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }
}
