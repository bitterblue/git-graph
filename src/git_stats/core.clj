(ns git-stats.core
  (:require [clj-jgit
             [porcelain :as jgit]
             [querying :as jgit-query]]
            [clojurewerkz.neocons.bolt :as neobolt]))

(defn- load-repo [path]
  (-> (jgit/discover-repo path)
      (jgit/load-repo)))

(defn commits
  "Returns a seq of commit-infos for all commits in the history of the current branch."
  [repo]
  (map (partial jgit-query/commit-info repo) (jgit/git-log repo)))

(defn import-commit-stmt [c]
  (let [props     {:id      (:id c)
                   :time    (.getTime (:time c))
                   :message (:message c)}
        files     (for [[f a] (:changed_files c)]
                    {:name f :action (name a)})
        author    {:name  (:author c)
                   :email (:email c)}
        committer {:name  (-> c :raw .getCommitterIdent .getName)
                   :email (-> c :raw .getCommitterIdent .getEmailAddress)}]
    ["MERGE (c:Commit {id: {props}.id})
      SET c = {props}
      WITH c
      MERGE (a {name: {author}.name, email: {author}.email})
      SET a :Author
      WITH c, a
      CREATE UNIQUE (a)-[:AUTHORED]->(c)
      WITH c
      MERGE (a {name: {committer}.name, email: {committer}.email})
      SET a :Committer
      WITH c, a
      CREATE UNIQUE (a)-[:COMMITTED]->(c)
      WITH c
      UNWIND {files} AS file
      MERGE (f:File {name: file.name})
      WITH c, f, file.action as a
      CREATE UNIQUE (c)-[:CHANGES {action: a}]->(f);"
     {:props     props
      :files     files
      :author    author
      :committer committer}]))

(defn import-parent-rel-stmts [c]
  (let [id      (:id c)
        parents (-> c :raw .getParents)]
    (map (fn [p] ["MATCH (a:Commit), (b:Commit)
                   WHERE a.id = {id} AND b.id = {pid}
                   CREATE UNIQUE (a)-[r:HAS_PARENT]->(b);"
                  {:id  id
                   :pid (-> p .getName str)}])
         parents)))

(def label-merge-commits-stmt
  ["MATCH (c:Commit)-[p:HAS_PARENT]->()
    WITH c, count(p) as parent_count
    WHERE parent_count > 1
    SET c :Merge;" {}])

;; TODO: merge authors with identical names
(def merge-authors-with-same-name-stmt
  ["MATCH (a:Author), (b:Author)
    WHERE a <> b AND a.name = b.name
    " {}])

(defn run-query! [session stmt]
  (let [[qry params] stmt]
    (neobolt/query session qry (clojure.walk/stringify-keys params))))

(defn import-graph [repo-path]
  (let [repo (load-repo repo-path)
        cs   (commits repo)]
    (with-open [conn    (neobolt/connect "bolt://localhost")
                session (neobolt/create-session conn)]
      (let [stmts (concat (map import-commit-stmt cs)
                          (mapcat import-parent-rel-stmts cs)
                          [label-merge-commits-stmt])]
        (dorun (map (partial run-query! session) stmts))))))

(defn count-commits
  "Counts the total number of commits, excluding merge commits."
  [session]
  (let [query  "MATCH (c:Commit) WHERE NOT c:Merge RETURN count(c) AS n;"
        result (neobolt/query session query)]
    (get (first result) "n")))

(defn pair-freqs
  "Returns a seq of file pairs and how often they occur. Only pairs occuring more
  than n times are counted."
  [session n]
  (let [query  "MATCH (a:File)<-[:CHANGES]-(c:Commit)-[:CHANGES]->(b:File)
                WHERE NOT c:Merge AND id(a) < id(b)
                WITH a, b, count(c) AS freq
                WHERE freq > {maxfreq}
                RETURN a.name AS a, b.name AS b, freq;"
        result (run-query! session [query {:maxfreq n}])]
    (clojure.walk/keywordize-keys result)))

(defn count-changes
  "Count the commits changing a specified file, excluding merge commits."
  [session f]
  (let [query  "MATCH (c:Commit)-[:CHANGES]->(f:File)
                WHERE f.name =  {filename} AND NOT c:Merge
                RETURN count(c) AS n;"
        result (run-query! session [query {:filename f}])]
    (get (first result) "n")))

(defn mcc
  "Calculate the Matthews Correlation Coefficient (MCC). The MCC ranges between -1 and 1,
  where 1 indicates a perfect agreement, 0 indicates no correlation and -1 complete
  disagreement."
  [n00 n01 n10 n11]
  (/ (- (* n11 n00) (* n10 n01))
     (Math/sqrt (* (+ n10 n11) (+ n00 n01) (+ n00 n10) (+ n01 n11)))))
