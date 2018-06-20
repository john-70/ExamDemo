package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.domain.Task;
import com.migu.schedule.info.TaskInfo;

import java.awt.print.Pageable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule {

    private Set<Integer> allNodes = Collections.synchronizedSet(new TreeSet<Integer>());
    private Map<Integer, Task> suspendTasks = new ConcurrentHashMap<Integer, Task>();
    private Map<Integer, Task> executeTasks = new ConcurrentHashMap<Integer, Task>();

    private boolean checkInt(int id) {
        return id <= 0;
    }

    private boolean existTask(int taskId) {
        return suspendTasks.containsKey(taskId) || executeTasks.containsKey(taskId);
    }

    private boolean existNode(int nodeId) {
        return allNodes.contains(nodeId);
    }

    public int init() {
        allNodes.clear();
        suspendTasks.clear();
        executeTasks.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (checkInt(nodeId)) return ReturnCodeKeys.E004;
        if (existNode(nodeId)) return ReturnCodeKeys.E005;

        allNodes.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (checkInt(nodeId)) return ReturnCodeKeys.E004;
        if (!existNode(nodeId)) return ReturnCodeKeys.E007;

        allNodes.remove(nodeId);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if (checkInt(taskId)) return ReturnCodeKeys.E009;
        if (existTask(taskId)) return ReturnCodeKeys.E010;

        suspendTasks.put(taskId, new Task(taskId, consumption, -1));
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (checkInt(taskId)) return ReturnCodeKeys.E009;
        if (!existTask(taskId)) return ReturnCodeKeys.E012;

        if (suspendTasks.containsKey(taskId)) suspendTasks.remove(taskId);
        else executeTasks.remove(taskId);

        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        if (checkInt(threshold)) return ReturnCodeKeys.E002;

        List<Task> tasks = new ArrayList<Task>(suspendTasks.size() + executeTasks.size());

        if (suspendTasks.size() > 0) {
            tasks.addAll(suspendTasks.values());
        } else if (executeTasks.size() <= 0) {
            return ReturnCodeKeys.E013;
        }

        tasks.addAll(executeTasks.values());
        calcPlan(tasks);

        return ReturnCodeKeys.E000;
    }

    private void calcPlan(List<Task> tasks) {
        Deque<Integer> nodes = new LinkedList<Integer>(this.allNodes);
        Collections.sort(tasks, new Comparator<Task>() {
            public int compare(Task o1, Task o2) {
                return o1.getConsumption() - o2.getConsumption();
            }
        });

        Map<Integer, List<Integer>> plan = new HashMap<Integer, List<Integer>>();
        int nodeSize = nodes.size();
        int taskSize = tasks.size();
        boolean asc = true;
        int tempNodeId;
        List<Integer> tempConsumptions;

        for (int i = 0; i < taskSize; i++) {
            if (asc) {
                tempNodeId = nodes.pollFirst();
            } else {
                tempNodeId = nodes.pollLast();
            }

            if (!plan.containsKey(tempNodeId)) {
                tempConsumptions = new ArrayList<Integer>();
                plan.put(tempNodeId, tempConsumptions);
            } else {
                tempConsumptions = plan.get(tempNodeId);
            }

            tempConsumptions.add(tasks.get(i).getConsumption());

            if (i % nodeSize == (nodeSize - 1)) {
                asc = !asc;
            }
        }
    }

    private void execPlan() {

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
