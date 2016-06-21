from task_manager import task_manager

# Define task
def sum_prod(fun):
  results = [0, 1]
  for i in range(len(fun)):
    results[0] += fun[i]
    results[1] *= fun[i]
  return results

# Create list of tasks
tasks = [[1.*i, 10.*i, 100.*i] for i in range(1000)]

# Send tasks to task manager
results = task_manager(tasks, sum_prod, "example.csv")
