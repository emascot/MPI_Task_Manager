program example
  use mpi
  implicit none
  integer, parameter :: Ntasks=10, Nfun=2, Nres=2, dp=kind(0.d0)
  integer :: rank, ierr
  real(dp) :: tasks(Nfun,Ntasks), results(Nres,Ntasks), output(Nfun+Nres,Ntasks)

  interface task_manager
    subroutine task_manager(tasks,Ntasks,Nfun,Nres,func,results,ierr,fname)
      integer, parameter :: dp=kind(0.d0)
      integer, intent(in) :: Ntasks,Nfun,Nres
      integer, intent(out) :: ierr
      real(dp), intent(in) :: tasks(Nfun,Ntasks)
      real(dp), intent(out) :: results(Nres,Ntasks)
      character(len=*), optional, intent(in) :: fname
      external :: func
    end subroutine task_manager
  end interface

! Initialize MPI
  call MPI_INIT(ierr)
  call MPI_COMM_RANK(MPI_COMM_WORLD,rank,ierr)

! Make task list
  call random_number(tasks)

  call task_manager(tasks,Ntasks,Nfun,Nres,sum_prod,results,ierr)

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
