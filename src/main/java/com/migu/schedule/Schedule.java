package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.domain.Task;
import com.migu.schedule.info.TaskInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule {

    private Set<Integer> nodes = Collections.synchronizedSet(new HashSet<Integer>());
    private Map<Integer, Task> suspendTasks = new ConcurrentHashMap<Integer, Task>();
    private Map<Integer, Task> executeTasks = new ConcurrentHashMap<Integer, Task>();

    private boolean checkId(int id) {
        return id <= 0;
    }

    private boolean existTask(int taskId) {
        return suspendTasks.containsKey(taskId) || executeTasks.containsKey(taskId);
    }

    private boolean existNode(int nodeId) {
        return nodes.contains(nodeId);
    }

    public int init() {
        nodes.clear();
        suspendTasks.clear();
        executeTasks.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (checkId(nodeId)) return ReturnCodeKeys.E004;
        if (existNode(nodeId)) return ReturnCodeKeys.E005;

        nodes.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (checkId(nodeId)) return ReturnCodeKeys.E004;
        if (!existNode(nodeId)) return ReturnCodeKeys.E007;

        nodes.remove(nodeId);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if (checkId(taskId)) return ReturnCodeKeys.E009;
        if (existTask(taskId)) return ReturnCodeKeys.E010;

        suspendTasks.put(taskId, new Task(taskId, consumption, -1));
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (checkId(taskId)) return ReturnCodeKeys.E009;
        if (!existTask(taskId)) return ReturnCodeKeys.E012;

        if (suspendTasks.containsKey(taskId)) suspendTasks.remove(taskId);
        else executeTasks.remove(taskId);

        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        // TODO 方法未实现
        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (tasks == null) return ReturnCodeKeys.E016;

        tasks.clear();
        for (Task task : suspendTasks.values()) {
            tasks.add(covertTask2TaskInfo(task));
        }

        for (Task task : executeTasks.values()) {
            tasks.add(covertTask2TaskInfo(task));
        }

        Collections.sort(tasks, new Comparator<TaskInfo>() {
            public int compare(TaskInfo o1, TaskInfo o2) {
                return o1.getTaskId() - o2.getTaskId();
            }
        });
        return ReturnCodeKeys.E015;
    }

    private TaskInfo covertTask2TaskInfo(Task task) {
        TaskInfo info = new TaskInfo();
        info.setTaskId(task.getTaskId());
        info.setNodeId(task.getNodeId());
        return info;
    }

}
