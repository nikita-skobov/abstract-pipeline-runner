# Abstract Pipeline Runner

This is a rust library that gives you an easy abstraction to "run" any hierarchy
of "nodes" you can think of, and supports running nodes sequentially or in parallel.

A node is a struct that can contain either a "task", or a series of more nodes, or a parallel list of more nodes.

a series of nodes get "run" in order, whereas a parallel list of nodes get spawned at the same time, one thread per node.

a "task" is an interface that you as the user of this library will implement to provide an actual way of
"doing work". Any data structure can be a "task", so long as it implements the methods provided in the `Task` trait
in this library. Each time a `Task` is called to "run", it gets provided an immutable view of the global state of the pipeline,
and the task can make dynamic decisions based on that.


This library does not contain any code to spawn subshells, or execute programs. It only provides a way to easily define
a hierarchy of steps, and to run those steps on 1 or more threads as defined by the hierarchy structure.


