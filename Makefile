run: clean server
	./server

include $(GOROOT)/src/Make.inc

TARG=server
GOFILES=\
		main.go\

include $(GOROOT)/src/Make.cmd
