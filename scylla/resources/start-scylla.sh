#!/bin/bash

export SCYLLA_HOME=/var/lib/scylla
export SCYLLA_CONF=/etc/scylla

/usr/bin/scylla --log-to-syslog 1 --log-to-stdout 0 --default-log-level info \
    --network-stack posix -m 8G --collectd 0 --poll-mode --developer-mode 1 --smp 4 \
    --experimental 1 > /dev/null 2>&1 &

/usr/lib/scylla/jmx/scylla-jmx -l /usr/lib/scylla/jmx/ > /dev/null 2>&1 &
