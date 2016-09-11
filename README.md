# MPI_Task_Manager
Task manager to complete tasks in parallel using MPI in Fortran and Python

Adapted from [https://github.com/robevans/MPI/](https://github.com/robevans/MPI/blob/master/example%20task%20farm.c)

## Usage

|Method      |Description|
|------------|-----------|
|task_manager|Main driver|
|task_farm   |Uses process 0 to distribute tasks as processes finish tasks|
|task_divide |Initially divide tasks evenly by mod(task #, size)=rank|

#### Fortran

* Create an array with dimensions (Nfun, Ntasks)
where Nfun is the number of function arguments of the task subroutine
and Ntasks is the number of total tasks to complete.
* Create a subroutine with arguments (Nfun, fun, Nres, res) 
where Nfun is the number of function arguments,
fun is the array of function arguments,
Nres is the number of results,
and res is the array of results

##### Execute

```bash
make
# X is the number of copies to run
mpirun -n X ./example.x
```

##### Example

```fortran
program example
  use parallel_tasks
  use mpi
  implicit none
  integer, parameter :: Ntasks=1000, Nfun=2, Nres=2, dp=kind(0.d0)
  integer :: i, rank, ierr
  real(dp) :: tasks(Nfun,Ntasks), results(Nres,Ntasks), output(Nfun+Nres,Ntasks)

  ! Initialize MPI
  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

  ! Make task list
  do i=1,Ntasks
    tasks(1,i) = 1._dp/i
    tasks(2,i) = i*i
  enddo

  ! Use tasks as input to sum_prod routine and split among processes
  call task_manager(Ntasks,Nfun,Nres,tasks,sum_prod,results,ierr)

  ! Print the tasks and results
  if ( rank.eq.0 ) then
    output(1:Nfun,:) = tasks(:,:)
    output(Nfun+1:Nfun+Nres,:) = results(:,:)
    write(6,'(4A20)') "x", "y", "sum", "product"
    write(6,'(4F20.10)') output
  end if

  ! Finalize MPI
  call MPI_FINALIZE(ierr)

contains
  subroutine sum_prod(Nfun,fun,Nres,res)
    integer, parameter :: dp=kind(0.d0)
    integer, intent(in) :: Nfun, Nres
    real(dp), intent(in) :: fun(Nfun)
    real(dp), intent(out) :: res(Nres)

    res(1) = fun(1) + fun(2)
    res(2) = fun(1) * fun(2)
  end subroutine sum_prod
end program example
```

#### Python

* Create a list of tasks
* Create a function that takes an element of tasks as the argument

##### Execute

```bash
# X is the number of copies to run
mpirun -n X python example.py
```

##### Example

```python
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
results = task_manager(tasks, sum_prod, "example.csv") # Output to file
results = task_manager(tasks, sum_prod) # Don't output to file

# Print results
if (rank == 0):
  for i in range(len(results)):
    print tasks[i], results[i]
```
