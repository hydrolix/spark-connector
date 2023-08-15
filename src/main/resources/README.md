## Notes about this directory

### turbine_cmd
This binary blob is proprietary, not open source. See the [relevant section](../../../README.md#proprietary) of the 
top-level README for more information.

### WyHash
WyHash is public domain. `linux-x86-64/libwyhash.so` and `darwin-x86-64/libwyhash.dylib` were built from 
[wyhash.c](./wyhash.c) in this directory. This version matches the behaviour observed in other components; many others 
don't.

How I built it:
```shell
clang -shared wyhash.c -o linux-x86-64/libwyhash.so
```
or:
```shell
clang -shared wyhash.c -o darwin-x86-64/libwyhash.so
```

We might prefer for this to be statically linked, but it's extremely dependency-light anyway.

See [WyHashTest.scala](../scala/io/hydrolix/spark/connector/WyHashTest.scala) for some test cases.

We could cross-build this for other OS's and architectures too, but at the moment [turbine_cmd](linux-x86-64/turbine_cmd) is 
`linux-x86-64`-only anyway so there's not much point at the moment.