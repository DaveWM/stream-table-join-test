(ns willa-demo.core
  (:require [jackdaw.streams :as streams]
            [willa.core :as w]
            [willa.utils :as wu]
            [loom.graph :as l]
            [willa.streams :as ws]
            [willa.specs :as wsp]
            [willa.workflow :as ww]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [willa-demo.utils :refer [merge-topologies]])
  (:import (org.apache.kafka.streams.kstream JoinWindows Suppressed Suppressed$BufferConfig TimeWindows)
           (java.time Duration)))


(def app-config
  {"application.id" "willa-test"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
         ws/default-serdes))

(def sentences-topic
  (->topic "sentences-topic"))
(def words-topic
  (->topic "words-topic"))
(def word-count-topic
  (->topic "word-count-topic"))

(def topics
  [sentences-topic
   words-topic
   word-count-topic])


(def split-words-xform
  (mapcat (fn [[key sentence]]
            (->> (str/split sentence #"\s+")
                 (map #(-> [key %]))))))


(def workflow
  [[:topic/sentences :stream/words]
   [:stream/words :topic/words]
   [:stream/words :table/word-count]
   [:table/word-count :topic/word-count]])

(def entities
  {:topic/sentences sentences-topic
   :topic/words words-topic
   :topic/word-count word-count-topic

   :stream/words {::w/entity-type :kstream
                  ::w/xform split-words-xform}

   :table/word-count {::w/entity-type :ktable
                      ::w/group-by-fn (fn [[k v]] v)
                      ::w/aggregate-initial-value 0
                      ::w/aggregate-adder-fn (fn [agg _]
                                               (inc agg))}})


(def topology
  {:workflow workflow
   :entities entities})


(defn start! []
  (let [builder (doto (streams/streams-builder)
                  (w/build-topology! topology))]
    (doto (streams/kafka-streams builder
                                 app-config)
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (println e))))
      streams/start)))



(comment

  ;; Setup

  (require 'jackdaw.client
           'jackdaw.admin
           '[clojure.core.async :as a])

  (def admin-client (jackdaw.admin/->AdminClient app-config))
  (jackdaw.admin/create-topics! admin-client topics)
  (jackdaw.admin/list-topics admin-client)
  (def app (start!))

  (defn reset []
    (streams/close app)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/delete-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/create-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (alter-var-root #'app (fn [_] (start!))))


  (def producer (jackdaw.client/producer app-config
                                         willa.streams/default-serdes))
  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         willa.streams/default-serdes))
  (def input-consumer (jackdaw.client/consumer (assoc app-config "group.id" "input-consumer")
                                               willa.streams/default-serdes))
  (jackdaw.client/subscribe consumer [word-count-topic])
  (jackdaw.client/subscribe input-consumer [sentences-topic])


  ;; Test out topology

  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord sentences-topic "key" "a simple sentence"))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord sentences-topic "key" "another simple sentence"))

  ;; input
  (do (jackdaw.client/seek-to-beginning-eager input-consumer)
      (->> (jackdaw.client/poll input-consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))

  ;; output
  (do (jackdaw.client/seek-to-beginning-eager consumer)
      (->> (jackdaw.client/poll consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))


  ;; Visualise topology

  (wv/view-topology topology)

  ;; Try out transducer on it's own

  (into [] split-words-xform [[:k "a simple sentence"]
                              [:k "another sentence"]])


  ;; Experiment

  (def experiment
    (we/run-experiment topology
                       {:topic/sentences [{:key "k" :value "a simple sentence" :timestamp 100}
                                          {:key "k" :value "another simple sentence" :timestamp 1200}]}))

  (we/results-only experiment)

  (wv/view-topology experiment)

  ;; Validate topology

  (s/valid? ::wsp/topology topology)

  (s/explain ::wsp/topology (update topology :workflow conj [:a :b]))


  ;; Joins

  (def topology-with-join
    {:entities {:input-a (->topic "a")
                :input-b (->topic "b")
                :output (->topic "output")}
     :workflow [[:input-a :output]
                [:input-b :output]]
     :joins {[:input-a :input-b] {::w/join-type :inner
                                  ::w/window (JoinWindows/of 1000)}}})


  (def experiment
    (we/run-experiment topology-with-join
                       {:input-a [{:key :k :value 1 :timestamp 100}]
                        :input-b [{:key :k :value 2 :timestamp 200}]}))

  (we/results-only experiment)

  (wv/view-topology experiment)


  ;; Serialize topology

  (def deserialized-topology
    (clojure.edn/read-string
      {:readers {'resolve (fn [s] (var-get (resolve s)))}}
      (slurp (io/resource "topology.edn"))))

  (wv/view-topology deserialized-topology)

  (-> (we/run-experiment deserialized-topology
                         {:topic/sentences [{:key :k :value "A short sentence"}]})
      (wv/view-topology))



  ;; Merge topologies - work in progress

  (def secondary-topology
    {:entities {:topic/secondary-sentences (->topic "secondary-sentences")
                :topic/word-count word-count-topic
                :stream/words {::w/entity-type :kstream
                               ::w/xform split-words-xform}

                :table/word-count {::w/entity-type :ktable
                                   ::w/group-by-fn (fn [[k v]] v)
                                   ::w/aggregate-initial-value 0
                                   ::w/aggregate-adder-fn (fn [agg _]
                                                            (inc agg))}}

     :workflow [[:topic/secondary-sentences :stream/words]
                [:stream/words :table/word-count]
                [:table/word-count :topic/word-count]]})


  (wv/view-topology secondary-topology)

  (wv/view-topology (merge-topologies topology secondary-topology))

  )
