(ns git-graph.loading
  "Utility functions for extracting data from a git repo and loading them into Neo4j."
  (:require [clj-jgit
             [porcelain :as jgit]
             [querying :as jgit-query]]
            [clojure.data.csv :as csv]
            [clojure.java
             [io :as io]
             [shell :refer [sh]]]
            [clojure.string :refer [split-lines]]))

;; jgit

(defn- load-repo [path]
  (-> (jgit/discover-repo path)
      (jgit/load-repo)))

(defn commits
  "Returns a seq of commit-infos for all commits in the history of the current branch."
  [repo]
  (map (partial jgit-query/commit-info repo) (jgit/git-log repo)))

;; TODO: include rename detection (jgit's RenameDetector)?


;; csv export

(defn write-csv [writer records]
  (when-first [record records]
    (let [header-line (apply str (interpose "," (map name (keys record))))]
      (.write writer header-line)
      (.write writer "\n")
      (csv/write-csv writer (map vals records)))))

(defn export-csv [repo csv-dir]
  (let [commit-props      (fn [c] {:id      (:id c)
                                   :time    (.getTime (:time c))
                                   :message (-> (:message c) split-lines first)})
        author            (fn [c] {:name (:author c) :email (:email c)})
        committer         (fn [c] {:name  (-> c :raw .getCommitterIdent .getName)
                                   :email (-> c :raw .getCommitterIdent .getEmailAddress)})
        authorships       (fn [person-fn]
                            (->> (commits repo)
                                 (group-by person-fn)
                                 (reduce-kv (fn [v a cs]
                                              (conj v (assoc a :commits (apply str (interpose ";" (mapv :id cs))))))
                                            [])))
        change-sets       (mapcat (fn [c]
                                    (mapv (fn [[f a]] {:commit (:id c) :name f :action (name a)})
                                          (:changed_files c)))
                                  (commits repo))
        ancestry          (map (fn [c] {:id      (:id c)
                                        :parents (->> (-> c :raw .getParents)
                                                      (map #(-> % .getName str))
                                                      (interpose ";")
                                                      (apply str))})
                               (commits repo))
        export-csv-thread (fn [{:keys [filename entries]}]
                            (future (with-open [writer (io/writer (str csv-dir "/" filename))]
                                      (write-csv writer @entries))))]
    (run! deref
          (mapv export-csv-thread [{:filename "commits.csv"
                                    :entries (delay (map commit-props (commits repo)))}
                                   {:filename "authorship.csv"
                                    :entries (delay (authorships author))}
                                   {:filename "commissions.csv"
                                    :entries (delay (authorships committer))}
                                   {:filename "changesets.csv"
                                    :entries (delay change-sets)}
                                   {:filename "ancestry.csv"
                                    :entries (delay ancestry)}]))))


;; load graph

(defn cypher-shell [s]
  (let [neo4j-container "gitgraph_neo4j_1"
        std-in          (if (string? s) (java.io.StringReader. s) s)]
    (sh "docker" "exec" "--interactive" neo4j-container "/var/lib/neo4j/bin/cypher-shell"
        :in std-in)))

(defn load-graph
  "Runs the import-repository.cypher script via cypher-shell in the neo4j docker container.
  The equivalent command line would be
      $ cat cypher/import-repository.cypher | docker exec -i <container> /var/lib/neo4j/bin/cypher-shell"
  [] (cypher-shell (io/reader "cypher/import-repository.cypher")))

