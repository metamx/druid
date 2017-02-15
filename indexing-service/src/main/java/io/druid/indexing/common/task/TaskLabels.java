package io.druid.indexing.common.task;

import com.google.common.base.Preconditions;

public class TaskLabels
{
  public static final String TASK_LABEL_FIELD = "label";

  public static String getTaskLabel(Task task) {
    Preconditions.checkNotNull(task, "task");
    Object taskLabel = task.getContext().get(TASK_LABEL_FIELD);
    return taskLabel == null ? null : (String) taskLabel;
  }
}
