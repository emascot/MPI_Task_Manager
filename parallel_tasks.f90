!> Distribute tasks to processes.
!> Tasks are an array of real(8) sent to a subroutine.
!> |option    |description                                |
!> |----------|-------------------------------------------|
!> |stream    |Write output as byte stream instead of text|
!> |sequential|Write after each task is completed         |
!> |prog_bar  |Show a progress bar with estimated time    |
!> \date July 2016
!> \author Eric Mascot
!==============================================================
!> \par Example code:
!> \code
!> program example
!>   use mpi
!>   use parallel_tasks
!>   implicit none
!>   integer, parameter :: Ntasks=10, Nfun=2, Nres=2, dp=kind(0.d0)
!>   real(dp) :: tasks(Nfun,Ntasks), results(Nres,Ntasks)
!>   integer :: ierr
!> 
!>   ! Initialize MPI
!>   call MPI_INIT(ierr)
!> 
!>   ! Make task list
!>   call random_number(tasks)
!>
!>   ! Turn on progress bar
!>   prog_bar = .true.
!>
!>   ! Assign tasks and write results to "example.dat"
!>   call task_manager(Ntasks,Nfun,Nres,tasks,sum_prod,results,ierr,"example.dat")
!> 
!>   ! Finalize MPI
!>   call MPI_FINALIZE(ierr)
!> 
!> contains
!>
!>   ! Define task
!>   subroutine sum_prod(Nfun,fun,Nres,res)
!>     integer, intent(in) :: Nfun, Nres
!>     real(dp), intent(in) :: fun(Nfun)
!>     real(dp), intent(out) :: res(Nres)
!> 
!>     res(1) = fun(1) + fun(2)
!>     res(2) = fun(1) * fun(2)
!>   end subroutine sum_prod
!> end program example
!> \endcode
!==============================================================

module parallel_tasks
  use mpi
  implicit none
  integer, parameter :: dp=kind(0.d0)
  ! true  - Write as byte stream (fast)
  ! false - Write in human readable format
  logical :: stream=.false.
  ! true  - Sequentially write after each task
  ! false - Do not do sequential write
  logical :: sequential=.false.
  ! true  - Show progress bar
  ! false - Do not show progress
  logical :: prog_bar=.false.
  save
  private
  public :: task_manager, task_divide, task_farm, stream, sequential, prog_bar
contains


!==============================================================
!  task_manager
!> Assigns a new task to a process when process finishes task.
!> Takes an array of inputs (tasks) and passes it through
!> an externally declared subroutine (func).
!
!> \param[in]  Ntasks  Number of tasks \n
!>                     Size of tasks array
!> \param[in]  Nfun    Number of function Arguments \n
!>                     Size of arguments array for subroutine "func"
!> \param[in]  Nres    Number of results \n
!>                     Size of results array from subroutine "func"
!> \param[in]  tasks   Array of function inputs \n
!>                     Arguments to use for subroutine "func"
!> \param[in]  func    externally declared subroutine to calculate tasks \n
!>                     Must have parameters (Nfun,fun,Nres,res)
!>                     - Inputs:
!>                       - Nfun - Size of fun array
!>                       - fun - Array of inputs
!>                       - Nres - Size of res array
!>                     - Output:
!>                       - res - Array of results
!> \param[out] results Array of results
!>                     Output of subroutine "func"
!> \param[out] ierr    MPI error return value
!>   - MPI_SUCCESS
!>   - No error; MPI routine completed successfully.
!>   - MPI_ERR_COMM
!>   - Invalid communicator. A common error is to use a null communicator in a call
!>     (not even allowed in MPI_Comm_rank).
!>   - MPI_ERR_COUNT
!>   - Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!>   - MPI_ERR_TYPE
!>   - Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!>   - MPI_ERR_TAG
!>   - Invalid tag argument. Tags must be non-negative; tags in a receive
!>     (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG.
!>     The largest tag value is available through the the attribute MPI_TAG_UB.
!>   - MPI_ERR_RANK
!>   - Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one;
!>     ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.
!> \param[in] fname    Filename of output \n
!>                     Skips if fname is not provided \n
!>                     File has first columns as tasks and last columns as results
!> \date July 2016
!> \author Eric Mascot
!==============================================================

subroutine task_manager(Ntasks,Nfun,Nres,tasks,func,results,ierr,fname)
  implicit none
  integer, intent(in) :: Ntasks, Nfun, Nres
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  external :: func
  real(dp), intent(out) :: results(Nres,Ntasks)
  integer, intent(out) :: ierr
  character(len=*), optional, intent(in) :: fname
  integer :: size
  real(dp) :: start

  ! Start timer
  start = MPI_Wtime()

  ! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)

  ! For small number of processes, use all processes for tasks
  if (size.lt.8) then
    call task_divide(Ntasks,Nfun,Nres,tasks,func,results,ierr,fname)
  else
    call task_farm(Ntasks,Nfun,Nres,tasks,func,results,ierr,fname)
  end if
end subroutine task_manager


!==============================================================
!  task_farm
!> Assigns a new task to a process when process finishes task.
!> Takes an array of inputs (tasks) and passes it through
!> an externally declared subroutine (func).
!
!> \param[in]  Ntasks  Number of tasks \n
!>                     Size of tasks array
!> \param[in]  Nfun    Number of function Arguments \n
!>                     Size of arguments array for subroutine "func"
!> \param[in]  Nres    Number of results \n
!>                     Size of results array from subroutine "func"
!> \param[in]  tasks   Array of function inputs \n
!>                     Arguments to use for subroutine "func"
!> \param[in]  func    externally declared subroutine to calculate tasks \n
!>                     Must have parameters (Nfun,fun,Nres,res)
!>                     - Inputs:
!>                       - Nfun - Size of fun array
!>                       - fun - Array of inputs
!>                       - Nres - Size of res array
!>                     - Output:
!>                       - res - Array of results
!> \param[out] results Array of results
!>                     Output of subroutine "func"
!> \param[out] ierr    MPI error return value
!>   - MPI_SUCCESS
!>   - No error; MPI routine completed successfully.
!>   - MPI_ERR_COMM
!>   - Invalid communicator. A common error is to use a null communicator in a call
!>     (not even allowed in MPI_Comm_rank).
!>   - MPI_ERR_COUNT
!>   - Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!>   - MPI_ERR_TYPE
!>   - Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!>   - MPI_ERR_TAG
!>   - Invalid tag argument. Tags must be non-negative; tags in a receive
!>     (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG.
!>     The largest tag value is available through the the attribute MPI_TAG_UB.
!>   - MPI_ERR_RANK
!>   - Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one;
!>     ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.
!> \param[in] fname    Filename of output \n
!>                     Skips if fname is not provided \n
!>                     File has first columns as tasks and last columns as results
!> \date July 2016
!> \author Eric Mascot
!==============================================================

subroutine task_farm(Ntasks,Nfun,Nres,tasks,func,results,ierr,fname)
  implicit none
  integer, intent(in) :: Ntasks, Nfun, Nres
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  external :: func
  real(dp), intent(out) :: results(Nres,Ntasks)
  integer, intent(out) :: ierr
  character(len=*), optional, intent(in) :: fname
  integer :: size, rank
  real(dp) :: start

  ! Start timer
  start = MPI_Wtime()

  ! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)
  ! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

  ! Otherwise use root as master
  if ( rank.eq.0 ) then
    ! Root process assigns task
    call assign_tasks
  else
    ! Worker processes receive tasks
    call receive_tasks
  end if

  ! Broadcast results so all processes have same results
  call MPI_BCAST(results,Ntasks*Nres,MPI_REAL8,0,MPI_COMM_WORLD,ierr)

  ! Write to file if present
  call export(Ntasks,Nfun,Nres,tasks,results,fname)
  
contains

  subroutine assign_tasks
    implicit none
    integer :: i, tag, source, status(MPI_STATUS_SIZE)
    real(dp) :: buffer(Nres)

    ! Assign processes first task
    do i=1,size-1
      call MPI_SEND(tasks(:,i), Nfun, MPI_REAL8, i, i, MPI_COMM_WORLD, ierr)
    end do

    ! Assign rest of tasks
    do i=size,Ntasks+size-1
      ! Wait for process to finish
      call MPI_RECV(buffer, Nres, MPI_REAL8, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
      ! Get number of process
      source = status(MPI_SOURCE)
      ! Get number of task
      tag = status(MPI_TAG)
      ! Save result
      results(:,tag) = buffer
      ! Write result
      if (sequential) call seq_write(Nfun,Nres,tasks(:,tag),results(:,tag),fname)
      ! Show progress bar
      if (prog_bar) call progress(real(i-size+1, dp)/real(Ntasks), start)
      if (i .le. Ntasks) then
        ! Send process next task
        call MPI_SEND(tasks(:,i), Nfun, MPI_REAL8, source, i, MPI_COMM_WORLD, ierr)
      else
        ! Send finish signal to process
        call MPI_SEND(tasks(:,1), Nfun, MPI_REAL8, source, i, MPI_COMM_WORLD, ierr)
      end if
    end do
  end subroutine assign_tasks

  subroutine receive_tasks
    implicit none
    integer :: tag, status(MPI_STATUS_SIZE)
    real(dp) :: task(Nfun), result(Nres)

    do
      ! Receive task
      call MPI_RECV(task, Nfun, MPI_REAL8, 0, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
      ! Get number of task
      tag = status(MPI_TAG)
      ! Exit if all tasks are finished
      if (tag.gt.Ntasks) exit
      ! Do task
      call func(Nfun,task,Nres,result)
      ! Send results
      call MPI_SEND(result, Nres, MPI_REAL8, 0, tag, MPI_COMM_WORLD, ierr)
    end do
  end subroutine receive_tasks
end subroutine task_farm

!==============================================================
!  task_divide
!> Assigns each process an equal number of tasks.
!> Takes an array of inputs (tasks) and passes it through
!> an externally declared subroutine (func).
!
!> \param[in]  Ntasks  Number of tasks \n
!>                     Size of tasks array
!> \param[in]  Nfun    Number of function Arguments \n
!>                     Size of arguments array for subroutine "func"
!> \param[in]  Nres    Number of results \n
!>                     Size of results array from subroutine "func"
!> \param[in]  tasks   Array of function inputs \n
!>                     Arguments to use for subroutine "func"
!> \param[in]  func    externally declared subroutine to calculate tasks \n
!>                     Must have parameters (Nfun,fun,Nres,res)
!>                     - Inputs:
!>                       - Nfun - Size of fun array
!>                       - fun - Array of inputs
!>                       - Nres - Size of res array
!>                     - Output:
!>                       - res - Array of results
!> \param[out] results Array of results
!>                     Output of subroutine "func"
!> \param[out] ierr    MPI error return value
!>   - MPI_SUCCESS
!>   - No error; MPI routine completed successfully.
!>   - MPI_ERR_COMM
!>   - Invalid communicator. A common error is to use a null communicator in a call
!>     (not even allowed in MPI_Comm_rank).
!>   - MPI_ERR_COUNT
!>   - Invalid count argument. Count arguments must be non-negative; a count of zero is often valid.
!>   - MPI_ERR_TYPE
!>   - Invalid datatype argument. May be an uncommitted MPI_Datatype (see MPI_Type_commit).
!>   - MPI_ERR_TAG
!>   - Invalid tag argument. Tags must be non-negative; tags in a receive
!>     (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_TAG.
!>     The largest tag value is available through the the attribute MPI_TAG_UB.
!>   - MPI_ERR_RANK
!>   - Invalid source or destination rank. Ranks must be between zero and the size of the communicator minus one;
!>     ranks in a receive (MPI_Recv, MPI_Irecv, MPI_Sendrecv, etc.) may also be MPI_ANY_SOURCE.
!> \param[in] fname    Filename of output \n
!>                     Skips if fname is not provided \n
!>                     File has first columns as tasks and last columns as results
!> \date July 2016
!> \author Eric Mascot
!==============================================================

subroutine task_divide(Ntasks,Nfun,Nres,tasks,func,results,ierr,fname)
  implicit none
  integer, intent(in) :: Ntasks, Nfun, Nres
  real(dp), intent(in) :: tasks(Nfun,Ntasks)
  external :: func
  real(dp), intent(out) :: results(Nres,Ntasks)
  integer, intent(out) :: ierr
  character(len=*), optional, intent(in) :: fname
  integer :: i, j, size, rank, source, status(MPI_STATUS_SIZE)
  real(dp) :: task(Nfun), result(Nres), buffer(Nres,Ntasks), start

  ! Start timer
  start = MPI_Wtime()

  ! Get number of processes
  call MPI_COMM_SIZE(MPI_COMM_WORLD,size,ierr)
  ! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

  ! Divide tasks
  do i=1+rank,Ntasks,size
    task = tasks(:,i)
    ! Do task
    call func(Nfun,task,Nres,result)
    ! Save result
    results(:,i) = result
    ! Write result
    if (sequential) call seq_write(Nfun,Nres,task,result,fname)
    ! Show progress bar
    if (prog_bar) call progress(real(i, dp)/real(Ntasks, dp), start)
  end do

  ! Merge results
  if (rank.eq.0) then
    do i=1,size-1
      ! Receive results
      call MPI_RECV(buffer, Nres*Ntasks, MPI_REAL8, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status, ierr)
      ! Get number of process
      source = status(MPI_SOURCE)
      ! Save results
      do j=1+source,Ntasks,size
        results(:,j) = buffer(:,j)
      end do
    end do
  else
    ! Send results to root
    call MPI_SEND(results, Nres*Ntasks, MPI_REAL8, 0, 0, MPI_COMM_WORLD, ierr)
  end if

  ! Broadcast results so all processes have same results
  call MPI_BCAST(results,Ntasks*Nres,MPI_REAL8,0,MPI_COMM_WORLD,ierr)

  ! Write to file if present
  call export(Ntasks,Nfun,Nres,tasks,results,fname)
end subroutine task_divide


!==============================================================
!  seq_write
!> Sequential write of tasks and results.
!> Write when tasks finish.
!
!> \param[in]  Nfun    Number of function Arguments \n
!>                     Size of arguments array for subroutine "func"
!> \param[in]  Nres    Number of results \n
!>                     Size of results array from subroutine "func"
!> \param[in]  task    Array of function inputs \n
!>                     Arguments to use for subroutine "func"
!> \param[out] result  Array of results
!>                     Output of subroutine "func"
!> \param[in]  fname   Filename of output \n
!>                     Skips if fname is not provided \n
!>                     File has first columns as tasks and last columns as results
!> \date July 2016
!> \author Eric Mascot
!==============================================================

subroutine seq_write(Nfun,Nres,task,result,fname)
  integer, intent(in) :: Nfun, Nres
  real(dp), intent(in) :: task(Nfun), result(Nres)
  character(len=*), optional, intent(in) :: fname
  integer :: rank, ierr
  character(len=64) :: fname_MPI

  ! Stop if sequential writing turned off
  if (.not. present(fname)) return

  ! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

  ! Add rank extension to filename
  write(fname_MPI,'(A,".",I0.3)') trim(fname), rank

  ! Export data to file
  if (stream) then
    ! Write as unformatted data
    open(10, file=fname_MPI, access='append',form='unformatted')
      write(10) task(:), result(:)
    close(10)
  else
    ! Write as human readable data
    open(10, file=fname_MPI, access='append')
      write(10,'(*(2x,f18.12))') task(:), result(:)
    close(10)
  end if
end subroutine seq_write


!==============================================================
!  export
!> Write tasks and results to file
!
!> \param[in]  Ntasks  Number of tasks \n
!>                     Size of tasks array
!> \param[in]  Nfun    Number of function Arguments \n
!>                     Size of arguments array for subroutine "func"
!> \param[in]  Nres    Number of results \n
!>                     Size of results array from subroutine "func"
!> \param[in]  tasks   Array of function inputs \n
!>                     Arguments to use for subroutine "func"
!> \param[out] results Array of results
!>                     Output of subroutine "func"
!> \param[in]  fname   Filename of output \n
!>                     Skips if fname is not provided \n
!>                     File has first columns as tasks and last columns as results
!> \date July 2016
!> \author Eric Mascot
!==============================================================

subroutine export(Ntasks,Nfun,Nres,tasks,results,fname)
  integer, intent(in) :: Ntasks, Nfun, Nres
  real(dp), intent(in) :: tasks(Nfun,Ntasks), results(Nres,Ntasks)
  character(len=*), optional, intent(in) :: fname
  integer :: i, rank, ierr

  ! Stop if no filename is present
  if (.not. present(fname)) return

  ! Get number of this process
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

  ! Export data to file
  if (rank.eq.0) then
    if (stream) then
      ! Write as unformatted data
      open(10, file=fname, status='replace', access='stream')
        do i=1,Ntasks
          write(10) tasks(:,i), results(:,i)
        end do
      close(10)
    else
      ! Write as human readable data
      open(10, file=fname, status='replace')
        do i=1,Ntasks
          write(10,'(*(2x,f18.12))') tasks(:,i), results(:,i)
        end do
      close(10)
    end if
  end if
end subroutine export

!==============================================================
! progress    Show progress bar
!   percent   Percent completed
!   start     Time of start from system_clock
!==============================================================

subroutine progress(percent, start)
  use iso_fortran_env
  implicit none
  integer, parameter :: w=30
  real(dp), intent(in) :: percent, start
  integer :: ticks
  real(dp) :: elapsed, remaining
  character(len=w+2) :: bar

  ticks = int(percent*w)
  if (ticks>w) ticks=w
  if (ticks<0) ticks=0

  elapsed   = MPI_Wtime()-start
  remaining = int(elapsed*(1.0/percent-1.0))
  bar  = "["//repeat("=",ticks)//repeat(" ",w-ticks)//"]"
  
  write(OUTPUT_UNIT,"(A,I3,'% ',I4,':',I2.2,' elapsed',I4,':',I2.2,' remaining')") &
    bar, int(percent*100), &
    int(elapsed)/3600, mod(int(elapsed)/60,60), &
    int(remaining)/3600, mod(int(remaining)/60,60)
  call flush(OUTPUT_UNIT)
end subroutine progress
end module parallel_tasks