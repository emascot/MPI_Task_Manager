FC = mpifort
FFLAGS = -O3 -g

all : example.f90 task_manager.o
	$(FC) $(FFLAGS) -o example.x example.f90 task_manager.o

task_manager.o : task_manager.f90
	$(FC) $(FFLAGS) -c task_manager.f90

clean:
	rm -r *.o *.x.dSYM *.x
