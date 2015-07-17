# cassandra

A set of Jepsen tests for Cassandra


## notes

The MV tests need a special Java driver.

The shaded driver available on trunk should be installed to your local maven repository as an artifact with coordinates [com.datastax.cassandra/cassandra-driver-core "2.2.0-rc2-SHADED"].

Then, a patched version of Cassaforte from the `trunk` branch at [GitHub](https://github.com/jkni/cassaforte/tree/trunk) should be `lein install`ed.
