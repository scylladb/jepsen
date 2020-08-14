# Scylla

This repository implements Jepsen tests for Scylla. If your machine is set up to run Jepsen tests, it may be possible to run them directly. It is likely easiest to use the Docker image.

## Introduction

Tests are implemented for logged batches, counters, LWTs, materialized views, CQL maps, and CQL sets. To view tests available, look in the `{feature}_test` files in the `test/scylla` directory.

## Configuration

Some parameters are available for tuning through environment variables.

- If set, `JEPSEN_COMPACTION_STRATEGY` will change the compaction strategy on all tables used. The default value is SizeTieredCompactionStrategy.
- If `JEPSEN_COMMITLOG_COMPRESSION` is set to "true" (case-insensitive), commitlog compression will be enabled during the tests.
- If set to any Java double value (such as "0.1"), `JEPSEN_SCALE` will adjust test duration. This parameter will sometimes change the logical behavior of tests, since some operation frequency cannot be reasonably scaled below a certain base. Use sparingly.
- Under normal lifecycle, the cluster is torn down at the end of a test. If `LEAVE_CLUSTER_RUNNING` is set to any non-empty value, the cluster will be left running at the end of the test. In addition, the running cluster will be stopped at the start of a test. This option is best used to debug a cluster after a single test.
- If the environment variable `JEPSEN_DISABLE_COORDINATOR_BATCHLOG` has any value, the coordinator batchlog will be disabled for materialized views.
- If the environment variable `JEPSEN_PHI_VALUE` is set, this will be provided as the phi value to the failure detector.
- If the environment variable `JEPSEN_DISABLE_HINTS` is set, hinted handoff will be disabled.

## Starting the Docker Container

A Docker container preconfigured to run Jepsen tests is available at `tjake/jepsen` on [Docker Hub](https://hub.docker.com/r/tjake/jepsen). Since it runs Docker inside Docker, it must be run with the privileged flag. A command like `docker run -it --privileged -v /home/jkni/git:/jkni-git tjake/jepsen` will start the container and attach to it as an interactive shell. Since you'll likely be running a newer version of Jepsen/C* tests than those available in the image, you'll want to share the directory containing your local Jepsen/C* clone with the container as in the example above.

## Environment Setup (within Docker container)

Some setup is necessary once you are within the Docker container. These steps may be eliminated as C* versions move forward and artifacts become available in central repository.

Testing 3.0 and above requires a shaded driver from the C* source tree.

The shaded driver available with C* should be installed to your local maven repository as an artifact with coordinates [com.datastax.cassandra/cassandra-driver-core "trunk-SHADED"].

Then, a patched version of Cassaforte from the `trunk` branch at [GitHub](https://github.com/jkni/cassaforte/tree/trunk) should be `lein install`ed. This can be done by cloning the repository

Lastly, a patched version of clj-ssh must be installed. This is available at [GitHub](https://github.com/jkni/clj-ssh/tree/trunk) and should be `lein install`ed. (aphyr: I think this is no longer necessary; the patch from this branch is in mainline clj-ssh as well)

## Running Tests

A whole category of tests can be run using the selectors defined in `project.clj`. For example, one could run `lein test :mv` to test materialized views. These tests are additive, so one could run `lein test :mv :lwt` to test materialized views and lightweight transactions.

To run an individual test, one can use a command like `lein test :only scylla.counter-test/cql-counter-inc-halves`.

To test builds based on 3.0 or above, one needs to activate the `trunk` profile that contains a dependency on the patched version of Cassaforte described above. For example, the individual test from above can be run like `lein with-profile +trunk :only scylla.counter-test/cql-counter-inc-halves`.
