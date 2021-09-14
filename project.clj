(defproject stream-table-join-test "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [willa "0.2.1"]
                 [org.clojure/core.async "0.4.500"]
                 ;; Added so we get to see the logs from Kafka client
                 [org.slf4j/slf4j-log4j12 "1.7.32"]]
  :repl-options {:init-ns stream-table-join-test.core})
