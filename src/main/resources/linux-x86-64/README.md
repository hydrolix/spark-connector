### WyHash Provenance

This was built from [wyhash.c](./wyhash.c) in this directory. This version works, many others don't.

How I built it:
```shell
clang -shared wyhash.c -o libwyhash.so
```
We might prefer for this to be statically linked, but it's extremely dependency-light anyway.

See [WyHashTest.scala](../../../test/scala/io/hydrolix/spark/connector/WyHashTest.scala) for some test cases.

We could cross-build this for other OS's too, but at the moment [turbine_cmd](../turbine_cmd) is only built for 
`linux-x86-64` anyway so there wouldn't be much point.