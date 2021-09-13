(ns stream-table-join-test.core
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
            [clojure.spec.alpha :as s])
  (:import (org.apache.kafka.streams.kstream JoinWindows Suppressed Suppressed$BufferConfig TimeWindows)
           (java.time Duration)))


(def app-config
  {"application.id"    "table-join-test"
   "bootstrap.servers" "localhost:9092"
   "max.task.idle.ms"  "60000"
   })


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
         ws/default-serdes))

(def input-topic
  (->topic "input-topic"))
(def table-topic
  (->topic "table-topic"))
(def output-topic
  (->topic "output-topic"))

(def topics
  [input-topic
   table-topic
   output-topic])

(def workflow
  [[:topic/input :stream/stream]
   [:topic/table :table/table]
   [:table/table :stream/log]
   [:stream/stream :stream/log]
   [:stream/log  :topic/output]])

(def entities
  {:topic/input  input-topic
   :topic/table  table-topic
   :topic/output output-topic

   :stream/stream {::w/entity-type :kstream}
   :stream/log  {::w/entity-type :kstream
                 ::w/xform (map (fn [kvp]
                                  (println kvp)
                                  kvp))}

   :table/table  {::w/entity-type :ktable}})

(def joins
  {[:stream/stream :table/table] {::w/join-type :left}})

(def topology
  {:workflow workflow
   :entities entities
   :joins joins})


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
  (def output-consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                                willa.streams/default-serdes))
  (def input-consumer (jackdaw.client/consumer (assoc app-config "group.id" "input-consumer")
                                               willa.streams/default-serdes))
  (jackdaw.client/subscribe output-consumer [output-topic])
  (jackdaw.client/subscribe input-consumer [input-topic])


  ;; Test out topology

  (def timestamp 123)
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic 0 (inc timestamp) "key" :stream))
  ;; wait a few seconds
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord table-topic 0 (dec timestamp) "key" :table))

  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic 1 (inc timestamp) "second-key" :stream))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord table-topic 1 (dec timestamp) "second-key" :table))

  ;; input
  (do (jackdaw.client/seek-to-beginning-eager input-consumer)
      (->> (jackdaw.client/poll input-consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))

  ;; output
  (do (jackdaw.client/seek-to-beginning-eager output-consumer)
      (->> (jackdaw.client/poll output-consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))


  ;; Visualise topology

  (wv/view-topology topology)

  ;; Validate topology

  (s/valid? ::wsp/topology topology)

  (s/explain ::wsp/topology topology)

  )
