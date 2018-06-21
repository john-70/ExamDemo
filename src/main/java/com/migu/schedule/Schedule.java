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

        List<Integer> nodes = new ArrayList<>(this.allNodes);

        tasks.addAll(executeTasks.values());

        Collections.sort(tasks, new Comparator<Task>() {
            public int compare(Task o1, Task o2) {
                if (o1.getConsumption() - o2.getConsumption() == 0) {
                    return o1.getTaskId() - o2.getTaskId();
                } else {
                    return o1.getConsumption() - o2.getConsumption();
                }
            }
        });

        List<List<Integer>> lstIndexConsumptions = calcPlan(tasks, nodes.size());

        int min = 0;
        int max = 0;
        for (Integer c : lstIndexConsumptions.get(0)) {
            min += c;
        }

        for (Integer c : lstIndexConsumptions.get(lstIndexConsumptions.size() - 1)) {
            max += c;
        }

        if ((max - min) > threshold) {
            return ReturnCodeKeys.E014;
        }

        execPlan(lstIndexConsumptions, tasks, nodes);

        return ReturnCodeKeys.E013;
    }

    private List<List<Integer>> calcPlan(List<Task> tasks, int nodeSize) {

        Deque<Task> taskDeque = new LinkedList<Task>(tasks);

        Map<Integer, List<Integer>> plan = new HashMap<Integer, List<Integer>>();
        List<List<Integer>> lstTempConsumptions;
        int taskSize = tasks.size();

        for (int i = 0; i < nodeSize; i++) {
            plan.put(i, new ArrayList<>());
        }

        List<Integer> tempConsumptions;

        for (int i = 0; i < taskSize; i++) {
            tempConsumptions = plan.get(0);
            tempConsumptions.add(taskDeque.pollLast().getConsumption());

            lstTempConsumptions = new ArrayList<>(plan.values());
            Collections.sort(lstTempConsumptions, new Comparator<List<Integer>>() {
                @Override
                public int compare(List<Integer> o1, List<Integer> o2) {
                    int sum1 = 0;
                    int sum2 = 0;

                    for (Integer o : o1) {
                        sum1 += o;
                    }

                    for (Integer o : o2) {
                        sum2 += o;
                    }

                    return sum1 - sum2;
                }
            });

            for(int j = 0; j < lstTempConsumptions.size(); j++) {
                plan.put(j, lstTempConsumptions.get(j));
            }
        }

        List<List<Integer>> lstIndexConsumptions = new ArrayList<>(plan.values());

        Map<List<Integer>, Integer> mapIndexConsumptions = new HashMap<>();
        int sumConsumption;
        for (List<Integer> consumptions : lstIndexConsumptions) {
            sumConsumption = 0;
            for (Integer consumption : consumptions) {
                sumConsumption += consumption;
            }

            mapIndexConsumptions.put(consumptions, sumConsumption);
        }


        Collections.sort(lstIndexConsumptions, new Comparator<List<Integer>>() {
            @Override
            public int compare(List<Integer> o1, List<Integer> o2) {
                int sum1 = mapIndexConsumptions.get(o1);
                int sum2 = mapIndexConsumptions.get(o2);

                if (sum1 == sum2) {
                    return o1.size() - o2.size();
                } else {
                    return sum1 - sum2;
                }
            }
        });

        return lstIndexConsumptions;
    }

    private void execPlan(List<List<Integer>> lstIndexConsumptions, List<Task> tasks, List<Integer> nodes) {
        List<Integer> consumptions;
        int nodeId;
        int consumption;
        Task task;
        for (int i = 0; i < lstIndexConsumptions.size(); i++) {
            consumptions = lstIndexConsumptions.get(i);
            nodeId = nodes.get(i);
            for (int j = 0; j < consumptions.size(); j++) {
                consumption = consumptions.get(j);
                for (int k = 0; k < tasks.size(); k++) {
                    task = tasks.get(k);
                    if (task.getConsumption() == consumption) {
                        task.setNodeId(nodeId);
                        tasks.remove(task);
                        break;
                    }
                }
            }
        }
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
