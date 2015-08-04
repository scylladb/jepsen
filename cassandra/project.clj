(defproject cassandra "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Cassandra"
  :url "http://github.com/riptano/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jmx "0.3.1"]
                 [jkni/jepsen "0.0.5-SNAPSHOT"]
                 [jkni/cassaforte "trunk-SNAPSHOT"]
                 [com.codahale.metrics/metrics-core "3.0.2"]]
  :profiles {:dev {:plugins [[test2junit "1.1.1"]]}}
  :test-selectors {:steady :steady
                   :bootstrap :bootstrap
                   :map :map
                   :set :set
                   :mv :mv
                   :batch-set :batch-set
                   :lwt :lwt
                   :decommission :decommission
                   :counter :counter
                   :clock :clock
                   :all (constantly true)})
