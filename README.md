# Jepsen Cassandra Testing

This repository contains tests to understand Cassandra guarantees and invariants
in various failure modes.

It uses the Jepsen library.

The `docker` directory contains tools to build a Docker container suitable for
running Jepsen tests. I often run this container using a command like
```
docker run -it --privileged -v /jepsen/directory/on/host:/host jkni/jepsen
```
to run Jepsen tests under development.

The Jepsen library has diverged from upstream; before running any tests, run
`lein install` from the jepsen directory.
