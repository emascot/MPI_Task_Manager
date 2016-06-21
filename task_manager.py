from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()

def task_manager(tasks, func, fname=None):
  # Start timer
  start = time.time()

  # Use all processes on tasks for small size
  if size < 8:
    results = task_divide(tasks, func)
  else:
    results = task_farm(tasks, func)

  # If filename is provided, write to file
  if fname is not None:
    with open(fname,'w') as f:
      for i in range(len(tasks)):
        l = map(lambda x: str(x), tasks[i]+results[i])
        f.write(", ".join(l)+"\n")

  # Print elapsed time
  t = int(time.time() - start)
  if rank == 0:
    print("Time: {}:{:02d}:{:02d}".format( t/3600, t/60%60, t%60 ))
  return results

def task_farm(tasks, func):
  def assign_tasks():
    # Assign first task to each process
    for i in range(1,size):
      comm.send(tasks[i], dest=i, tag=i)

    # Assign rest of tasks
    for i in range(size, len(tasks)+size-1):
      # Wait for process to finish and receive results
      buf = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
      # Get rank of sender
      source = status.Get_source()
      # Get number of task
      tag = status.Get_tag()
      # Save result
      results[tag] = buf
      if (i < len(tasks)):
        # Send next task
        comm.send(tasks[i], dest=source, tag=i)
      else:
        # Send finish signal
        comm.send(tasks[0], dest=source, tag=i)

  def receive_tasks():
    while(True):
      # Receive task
      task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
      # Get number of task
      tag = status.Get_tag()
      # Exit if all tasks are finished
      if (tag >= len(tasks)): break
      # Send results
      comm.send(func(task), dest=0, tag=tag)

  # Initialize results
  results = [[] for i in range(len(tasks))]
  if (rank == 0):
    # Master process assigns tasks
    assign_tasks()
  else:
    # Workers do tasks until finished
    receive_tasks()

  # Broadcast so all processes have same results
  results = comm.bcast(results, root=0)
  return results

def task_divide(tasks, func):
  # Initialize results
  results = [[] for i in range(len(tasks))]
  # Divide up tasks
  for i in range(rank,len(tasks),size):
    results[i] = func(tasks[i])
  # Return if no other processes
  if (size == 1): return results
  
  # Master process merges results
  if (rank == 0):
    for i in range(size-1):
      # Receive results
      buf = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
      # Get rank of sender
      source = status.Get_source()
      # Save results
      for j in range(source, len(tasks), size):
        results[j] = buf[j]
  else:
    # Send results to master process
    comm.send(results, dest=0, tag=0)

  # Broadcast so all processes have same results
  results = comm.bcast(results, root=0)
  return results

# Example use
if __name__ == '__main__':
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

  # Print results
  if (rank == 0):
    for i in range(len(results)):
      print tasks[i], results[i]
