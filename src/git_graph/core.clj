(ns git-graph.core
  (:require [clj-time.core :as t]
            [clojurewerkz.neocons.bolt :as neobolt]
            [incanter
             [core :refer [matrix]]
             [stats :as stats]]
            [clj-time.coerce :refer [to-long]]))

;; Neo4j bolt queries

(defn run-query!
  ([session qry] (run-query! session qry nil))
  ([session qry params]
   (clojure.walk/keywordize-keys
    (neobolt/query session qry (clojure.walk/stringify-keys params)))))

(defmacro with-local-bolt-sess [& body]
  `(with-open [conn#  (neobolt/connect "bolt://localhost")
               ~'sess (neobolt/create-session conn#)]
     ~@body))


;; analyses

(defn count-commits
  "Counts the total number of commits, excluding merge commits."
  [session]
  (let [query  "MATCH (c:Commit) WHERE NOT c:Merge RETURN count(c) AS n;"
        result (run-query! session query)]
    (-> result first :n)))

(defn pair-freqs
  "Returns a seq of file pairs and how often they occur. Only pairs occuring more
  than n times are returned, and only a number up to the specified limit."
  [session n limit]
  (let [query  "MATCH (a:File)<-[:CHANGES]-(c:Commit)-[:CHANGES]->(b:File)
                WHERE NOT c:Merge
                      AND NOT a:Test AND NOT b:Test
                      // AND a.name ENDS WITH '.java' AND b.name ENDS WITH '.java'
                      AND id(a) < id(b)
                WITH a, b, count(c) AS freq
                WHERE freq > {maxfreq}
                RETURN a.name AS a, b.name AS b, freq
                ORDER BY freq DESC
                LIMIT {limit};"
        result (run-query! session query {:maxfreq n :limit limit})]
    result))

(defn count-changes
  "Count the commits changing a specified file, excluding merge commits."
  [session f]
  (let [query  "MATCH (c:Commit)-[:CHANGES]->(f:File)
                WHERE f.name =  {filename} AND NOT c:Merge
                RETURN count(c) AS n;"
        result (run-query! session query {:filename f})]
    (-> result first :n)))

(defn mcc
  "Calculate the Matthews Correlation Coefficient (MCC). The MCC indicates the
  strength of an association and ranges between -1 and 1, where 1 indicates a
  perfect agreement, 0 indicates no correlation and -1 complete
  disagreement."
  [n00 n01 n10 n11]
  (/ (- (* n11 n00) (* n10 n01))
     (Math/sqrt (* (+ n10 n11) (+ n00 n01) (+ n00 n10) (+ n01 n11)))))

(defn coevolution-analysis [session pairs]
  (let [num-commits   (count-commits session)
        count-changes (memoize (partial count-changes session))
        calculate-mcc (fn [{:keys [a b freq]}]
                        (let [only-a  (- (count-changes a) freq)
                              only-b  (- (count-changes b) freq)
                              neither (- num-commits only-a only-b freq)
                              chi-sq  (stats/chisq-test :table (matrix [[neither only-a] [only-b freq]]))]
                          {:only-a  only-a
                           :only-b  only-b
                           :neither neither
                           :both    freq
                           :mcc     (mcc neither only-a only-b freq)
                           :chi-sq  (:X-sq chi-sq)
                           :phi-coefficient (Math/sqrt (/ (:X-sq chi-sq) num-commits))
                           :p-value (:p-value chi-sq)}))]
    (into [] (map (fn [p] (merge p (calculate-mcc p)))) pairs)))

(defn hotspot-analysis
  "Returns the n most frequently changed files in the specified interval."
  ([session n] (hotspot-analysis session n (t/interval (t/epoch) (t/now))))
  ([session n interval]
   (let [query "MATCH (c:Commit)-->(f:File)
               WHERE {start} < c.time AND c.time < {end}
               WITH f.name AS name, count(c) AS num_changes
               RETURN name, num_changes
               ORDER BY num_changes DESCENDING
               LIMIT {n};"
         result (run-query! session query
                            {:n     n
                             :start (to-long (t/start interval))
                             :end   (to-long (t/end interval))})]
     result)))

(defn busfactor-analysis
  "Returns a list of n files and their authors where the number of authors is no
  larger than the specified maximum, 1 by default. The author nodes of the graph
  may have to be cleaned up first."
  ([session n] (busfactor-analysis session n 1))
  ([session n max]
   (let [query "MATCH (a:Author)-->(c:Commit)-->(f:File)
               WITH f.name AS name, collect(a.name) AS authors, count(a) AS busfactor
               WHERE busfactor <= {max}
               RETURN name, busfactor, authors
               ORDER BY busfactor ASCENDING
               LIMIT {n}"
         result (run-query! session query
                            {:n   n
                             :max max})]
     result)))

;; more possible metrics
;; * testing quotient (how strictly are changes to main accompanied by changes to test?)
;;   could indicate either lacking test coverage or a rigidity hazard caused by
;;   the test suite that will make refactoring expensive
