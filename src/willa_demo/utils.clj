(ns willa-demo.utils)


(defn merge-topologies [t1 t2]
  {:entities (merge (:entities t1) (:entities t2))
   :workflow (->> (concat (:workflow t1) (:workflow t2))
                  distinct
                  vec)
   :joins (merge (:joins t1) (:joins t2))})