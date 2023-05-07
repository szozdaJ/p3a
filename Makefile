all: psort.c 
	gcc -g -Wall -o psort psort.c

clean: 
	  $(RM) psort
