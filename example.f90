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
