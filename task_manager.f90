! FORTRAN MPI Task Manager by Eric Mascot
! adapted from
! https://github.com/robevans/MPI/blob/master/example%20task%20farm.c
!
! task_manager(tasks,Ntasks,Nfun,Nres,func,results,ierr,fname)
!     Assigns a new task to a process when process finishes task.
!     Takes an array of inputs (tasks) and passes it through
!     an externally declared subroutine (func).
!
! tasks(input) real(kind=8) dimension(Nfun,Ntasks) 
!     Array of function inputs
!     Arguments to use for subroutine "func"
!
! Ntasks(input) integer 
!     Number of tasks
!     Size of tasks array
!
! Nfun(input) integer 
!     Number of function Arguments
!     Size of arguments array for subroutine "func"
!
! Nres(input) integer 
!     Number of results
!     Size of results array from subroutine "func"
!
! func(input) externally declared subroutine to calculate tasks
!     external
!     Must have parameters (Nfun,fun,Nres,res)
!     Inputs:
!       Nfun - Size of fun array
!       fun - Array of inputs
!       Nres - Size of res array
!     Output:
!       res - Array of results
!
! results(output) real(kind=8) dimension(Nres,Ntasks) 
!     Array of results
!     Output of subroutine "func"
!
! ierr(output) integer 
!     MPI error return value
!     MPI_SUCCESS
!     No error; MPI routine completed successfully.
!     MPI_ERR_COMM
!     Invalid communicator. A common error is to use a null communicator in a call (not even allowed in MPI_Comm_rank).
!     MPI_ERR_COUNT
!     Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!     MPI_ERR_TYPE
!     Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!     MPI_ERR_TAG
!     Invalid tag argument. Tags must be non-negative; tags in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG. The largest tag value is available through the the attribute MPI_TAG_UB.
!     MPI_ERR_RANK
!     Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one; ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.
!
! fname(input) character(len=*) optional
!     Filename of output
!     Skips to file if fname is not provided
!     File has first columns as tasks and last columns as results
!
!============================================================
!
! program example
!   use mpi
!   implicit none
!   integer, parameter :: Ntasks=10, Nfun=2, Nres=2, dp=kind(0.d0)
!   integer :: ierr
!   real(dp) :: tasks(Nfun,Ntasks), results(Nres,Ntasks), output(Nfun+Nres,Ntasks)
! 
! ! Initialize MPI
!   call MPI_INIT(ierr)
! 
! ! Make task list
!   call random_number(tasks)
!
! ! Assign tasks and write results to "example.dat"
!   call task_manager(tasks,Ntasks,Nfun,Nres,sum_prod,results,ierr,"example.dat")
! 
! ! Finalize MPI
!   call MPI_FINALIZE(ierr)
! 
! contains
!
! ! Define task
!   subroutine sum_prod(Nfun,fun,Nres,res)
!     integer, parameter :: dp=kind(0.d0)
!     integer, intent(in) :: Nfun, Nres
!     real(dp), intent(in) :: fun(Nfun)
!     real(dp), intent(out) :: res(Nres)
! 
!     res(1) = fun(1) + fun(2)
!     res(2) = fun(1) * fun(2)
!   end subroutine sum_prod
! end program example

subroutine task_manager(tasks,Ntasks,Nfun,Nres,func,results,ierr,fname)
  use mpi
  implicit none
! true  - Write as byte stream (fast)
! false - Write in human readable format
  logical, parameter :: stream=.false.
  integer, parameter :: dp=kind(0.d0)
  integer, intent(in) :: Ntasks,Nfun,Nres
  integer, intent(out) :: ierr
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  real(dp), intent(out) :: results(Nres,Ntasks)
  character(len=*), optional, intent(in) :: fname
  external :: func
  integer :: i, start, size, rank

  interface task_divide
    subroutine task_divide(tasks,Ntasks,Nfun,Nres,func,results,ierr)
      integer, parameter :: dp=kind(0.d0)
      integer, intent(in) :: Ntasks,Nfun,Nres
      integer, intent(out) :: ierr
      real(dp), intent(in) :: tasks(Nfun,Ntasks)
      real(dp), intent(out) :: results(Nres,Ntasks)
      external :: func
    end subroutine task_divide
  end interface

  interface task_farm
    subroutine task_farm(tasks,Ntasks,Nfun,Nres,func,results,ierr)
      integer, parameter :: dp=kind(0.d0)
      integer, intent(in) :: Ntasks,Nfun,Nres
      integer, intent(out) :: ierr
      real(dp), intent(in) :: tasks(Nfun,Ntasks)
      real(dp), intent(out) :: results(Nres,Ntasks)
      external :: func
    end subroutine task_farm
  end interface

! Start timer
  call system_clock(start)

! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)
! Get rank of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

! For small number of processes, use all processes for tasks
  if (size.lt.8) then
    call task_divide(tasks,Ntasks,Nfun,Nres,func,results,ierr)
  else
    call task_farm(tasks,Ntasks,Nfun,Nres,func,results,ierr)
  end if

! Export data to file
  if (rank.eq.0 .and. present(fname)) then
    if (stream) then
!     Write as unformatted data
      open(10, file=fname, status="replace", access="stream")
        do i=1,Ntasks
          write(10) tasks(:,i), results(:,i)
        end do
      close(10)
    else
!     Write as human readable data
      open(10, file=fname, status="replace")
        do i=1,Ntasks
          write(10,*) tasks(:,i), results(:,i)
        end do
      close(10)
    end if
  end if
end subroutine task_manager


! task_farm(tasks,Ntasks,Nfun,Nres,func,results,ierr)
!     Assigns a new task to a process when process finishes task.
!     Takes an array of inputs (tasks) and passes it through
!     an externally declared subroutine (func).
!
! tasks(input) real(kind=8) dimension(Nfun,Ntasks) 
!     Array of function inputs
!     Arguments to use for subroutine "func"
!
! Ntasks(input) integer 
!     Number of tasks
!     Size of tasks array
!
! Nfun(input) integer 
!     Number of function Arguments
!     Size of arguments array for subroutine "func"
!
! Nres(input) integer 
!     Number of results
!     Size of results array from subroutine "func"
!
! func(input) externally declared subroutine to calculate tasks
!     external
!     Must have parameters (Nfun,fun,Nres,res)
!     Inputs:
!       Nfun - Size of fun array
!       fun - Array of inputs
!       Nres - Size of res array
!     Output:
!       res - Array of results
!
! results(output) real(kind=8) dimension(Nres,Ntasks) 
!     Array of results
!     Output of subroutine "func"
!
! ierr(output) integer 
!     MPI error return value
!     MPI_SUCCESS
!     No error; MPI routine completed successfully.
!     MPI_ERR_COMM
!     Invalid communicator. A common error is to use a null communicator in a call (not even allowed in MPI_Comm_rank).
!     MPI_ERR_COUNT
!     Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!     MPI_ERR_TYPE
!     Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!     MPI_ERR_TAG
!     Invalid tag argument. Tags must be non-negative; tags in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG. The largest tag value is available through the the attribute MPI_TAG_UB.
!     MPI_ERR_RANK
!     Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one; ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.

subroutine task_farm(tasks,Ntasks,Nfun,Nres,func,results,ierr)
  use mpi
  implicit none
  integer, parameter :: dp=kind(0.d0)
  integer, intent(in) :: Ntasks,Nfun,Nres
  integer, intent(out) :: ierr
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  real(dp), intent(out) :: results(Nres,Ntasks)
  external :: func
  integer :: size, rank, start

  interface progress
    subroutine progress(percent, start)
      integer, intent(in) :: start
      real, intent(in) :: percent
    end subroutine progress
  end interface

! Start timer
  call system_clock(start)

! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)
! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

! Otherwise use root as master
  if ( rank.eq.0 ) then
!   Root process assigns task
    call assign_tasks
  else
!   Worker processes receive tasks
    call receive_tasks
  end if

! Broadcast results so all processes have same results
  call MPI_BCAST(results,Ntasks*Nres,MPI_REAL8,0,MPI_COMM_WORLD,ierr)
  
contains

  subroutine assign_tasks
    implicit none
    integer :: i, tag, source, status(MPI_STATUS_SIZE)
    real(dp) :: buffer(Nres)

!   Assign processes first task
    do i=1,size-1
      call MPI_SEND(tasks(:,i), Nfun, MPI_REAL8, i, i, MPI_COMM_WORLD, ierr)
    end do

!   Assign rest of tasks
    do i=size,Ntasks+size-1
!     Wait for process to finish
      call MPI_RECV(buffer, Nres, MPI_REAL8, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
!     Get number of process
      source = status(MPI_SOURCE)
!     Get number of task
      tag = status(MPI_TAG)
!     Save result
      results(:,tag) = buffer
      call progress(real(i-size+1)/real(Ntasks), start)
      if (i .le. Ntasks) then
!       Send process next task
        call MPI_SEND(tasks(:,i), Nfun, MPI_REAL8, source, i, MPI_COMM_WORLD, ierr)
      else
!       Send finish signal to process
        call MPI_SEND(tasks(:,1), Nfun, MPI_REAL8, source, i, MPI_COMM_WORLD, ierr)
      end if
    end do
  end subroutine assign_tasks

  subroutine receive_tasks
    implicit none
    integer :: tag, status(MPI_STATUS_SIZE)
    real(dp) :: task(Nfun), result(Nres)

    do
!     Receive task
      call MPI_RECV(task, Nfun, MPI_REAL8, 0, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
!     Get number of task
      tag = status(MPI_TAG)
!     Exit if all tasks are finished
      if (tag.gt.Ntasks) exit
!     Do task
      call func(Nfun,task,Nres,result)
!     Send results
      call MPI_SEND(result, Nres, MPI_REAL8, 0, tag, MPI_COMM_WORLD, ierr)
    end do
  end subroutine receive_tasks
end subroutine task_farm



! task_divide(tasks,Ntasks,Nfun,Nres,func,results,ierr)
!     Assigns each process an equal number of tasks.
!     Takes an array of inputs (tasks) and passes it through
!     an externally declared subroutine (func).
!
! tasks(input) real(kind=8) dimension(Nfun,Ntasks) 
!     Array of function inputs
!     Arguments to use for subroutine "func"
!
! Ntasks(input) integer 
!     Number of tasks
!     Size of tasks array
!
! Nfun(input) integer 
!     Number of function Arguments
!     Size of arguments array for subroutine "func"
!
! Nres(input) integer 
!     Number of results
!     Size of results array from subroutine "func"
!
! func(input) externally declared subroutine to calculate tasks
!     external
!     Must have parameters (Nfun,fun,Nres,res)
!     Inputs:
!       Nfun - Size of fun array
!       fun - Array of inputs
!       Nres - Size of res array
!     Output:
!       res - Array of results
!
! results(output) real(kind=8) dimension(Nres,Ntasks) 
!     Array of results
!     Output of subroutine "func"
!
! ierr(output) integer 
!     MPI error return value
!     MPI_SUCCESS
!     No error; MPI routine completed successfully.
!     MPI_ERR_COMM
!     Invalid communicator. A common error is to use a null communicator in a call (not even allowed in MPI_Comm_rank).
!     MPI_ERR_COUNT
!     Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!     MPI_ERR_TYPE
!     Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!     MPI_ERR_TAG
!     Invalid tag argument. Tags must be non-negative; tags in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG. The largest tag value is available through the the attribute MPI_TAG_UB.
!     MPI_ERR_RANK
!     Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one; ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.

subroutine task_divide(tasks,Ntasks,Nfun,Nres,func,results,ierr)
  use mpi
  implicit none
  integer, parameter :: dp=kind(0.d0)
  integer, intent(in) :: Ntasks,Nfun,Nres
  integer, intent(out) :: ierr
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  real(dp), intent(out) :: results(Nres,Ntasks)
  external :: func
  integer :: size, rank, i, j, source, status(MPI_STATUS_SIZE), start
  real(dp) :: task(Nfun), result(Nres), buffer(Nres,Ntasks)

  interface progress
    subroutine progress(percent, start)
      integer, intent(in) :: start
      real, intent(in) :: percent
    end subroutine progress
  end interface

! Start timer
  call system_clock(start)

! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)
! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

! Divide tasks
  do i=1+rank,Ntasks,size
    task = tasks(:,i)
!   Do task
    call func(Nfun,task,Nres,result)
!   Save result
    results(:,i) = result
    call progress(real(i)/real(Ntasks), start)
  end do

! Merge results
  if (rank.eq.0) then
    do i=1,size-1
!     Receive results
      call MPI_RECV(buffer, Nres*Ntasks, MPI_REAL8, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
!     Get number of process
      source = status(MPI_SOURCE)
!     Save results
      do j=1+source,Ntasks,size
        results(:,j) = buffer(:,j)
      end do
    end do
  else
!   Send results to root
    call MPI_SEND(results, Nres*Ntasks, MPI_REAL8, 0, 0, MPI_COMM_WORLD, ierr)
  end if

! Broadcast results so all processes have same results
  call MPI_BCAST(results,Ntasks*Nres,MPI_REAL8,0,MPI_COMM_WORLD,ierr)
end subroutine task_divide


! progress(percent,start)
!     Show progress bar
!
! percent(input) real
!     Percent completed
!
! start(input) integer
!     Time of start from system_clock

subroutine progress(percent, start)
  use iso_fortran_env
  implicit none
  integer, parameter  :: w=30
  integer, intent(in) :: start
  real, intent(in)    :: percent
  integer             :: ticks, end, rate, elapsed, remaining
  character(len=w+2)  :: bar

  ticks = int(percent*w)
  if (ticks>w) ticks=w
  if (ticks<0) ticks=0

  call system_clock(end, rate)
  elapsed   = int(real(end-start)/real(rate))
  remaining = int(elapsed*(1.0/percent-1.0))
  bar  = "["//repeat("=",ticks)//repeat(" ",w-ticks)//"]"
  
  write(OUTPUT_UNIT,"(A,I3,'% ',I4,':',I2.2,' elapsed',I4,':',I2.2,' remaining')") &
    bar, int(percent*100), elapsed/3600, mod(elapsed/60,60), &
    remaining/3600, mod(remaining/60,60)
  call flush(OUTPUT_UNIT)
end subroutine progress
