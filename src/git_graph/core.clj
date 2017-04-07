(ns git-graph.core
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
    SET c :Merge;"])

(def label-test-files-stmt
  ["MATCH (f:File)
    WHERE f.name CONTAINS '/test/'
    SET f :Test;"])

(def merge-authors-with-same-name-stmt
  ;; FIXME: This fails if an author appears more than twice, because during the
  ;; first merge two nodes will be replaced by a new one, so during the second
  ;; merge, one of the originally matched nodes will no longer exist. This
  ;; results in a NotFoundException for that node.
  ["MATCH (a:Author), (b:Author)
    WHERE a.name = b.name AND id(a) < id(b)
    CALL apoc.refactor.mergeNodes([a,b]) YIELD node
    RETURN node;"])

(defn run-query!
  ([session qry] (run-query! session qry nil))
  ([session qry params]
   (neobolt/query session qry (clojure.walk/stringify-keys params))))

(defn run-statements! [conn stmts]
  (with-open [session (neobolt/create-session conn)]
    (doseq [[qry params] stmts]
      (run-query! session qry params))))


;; TODO: rewrite import to run in parallel, first for the list of commits,
;;       then files, then authors, creating historical relations last
;;       --> extract, transform, load


(defn import-graph [repo-path]
  (let [repo (load-repo repo-path)
        cs   (commits repo)]
    (with-open [conn (neobolt/connect "bolt://localhost")]
      (let [grain-size    250
            run-parallel! (fn [stmts]
                            (dorun (pmap (partial run-statements! conn)
                                         (partition-all grain-size stmts))))]
        (println "Extracting commits ...")
        (run-parallel! (map import-commit-stmt cs))
        (println "Extracting history relations ...")
        (run-parallel! (mapcat import-parent-rel-stmts cs))
        (println "Post-processing the graph ...")
        (run-statements! conn [label-merge-commits-stmt
                               label-test-files-stmt
                               merge-authors-with-same-name-stmt])))))

(defn count-commits
  "Counts the total number of commits, excluding merge commits."
  [session]
  (let [query  "MATCH (c:Commit) WHERE NOT c:Merge RETURN count(c) AS n;"
        result (run-query! session query)]
    (get (first result) "n")))

(defn pair-freqs
  "Returns a seq of file pairs and how often they occur. Only pairs occuring more
  than n times are returned, and only a number up to the specified limit."
  [session n limit]
  (let [query  "MATCH (a:File)<-[:CHANGES]-(c:Commit)-[:CHANGES]->(b:File)
                WHERE NOT c:Merge
                      AND NOT a:Test AND NOT b:Test
                      AND a.name ENDS WITH '.java' AND b.name ENDS WITH '.java' AND id(a) < id(b)
                WITH a, b, count(c) AS freq
                WHERE freq > {maxfreq}
                RETURN a.name AS a, b.name AS b, freq
                ORDER BY freq DESC
                LIMIT {limit};"
        result (run-query! session query {:maxfreq n :limit limit})]
    (clojure.walk/keywordize-keys result)))

(defn count-changes
  "Count the commits changing a specified file, excluding merge commits."
  [session f]
  (let [query  "MATCH (c:Commit)-[:CHANGES]->(f:File)
                WHERE f.name =  {filename} AND NOT c:Merge
                RETURN count(c) AS n;"
        result (run-query! session query {:filename f})]
    (get (first result) "n")))

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
                              neither (- num-commits only-a only-b freq)]
                          {:only-a  only-a
                           :only-b  only-b
                           :neither neither
                           :both    freq
                           :mcc     (mcc neither only-a only-b freq)}))]
    (into [] (map (fn [p] (merge p (calculate-mcc p)))) pairs)))

;; TODO: implement chi² test and check p-values


(defmacro with-local-bolt-sess [& body]
  `(with-open [conn#  (neobolt/connect "bolt://localhost")
               ~'sess (neobolt/create-session conn#)]
     ~@body))

;; more possible metrics
;; * most frequently changed files (hotspot analysis)
;;   MATCH (f:File)<--(c:Commit) WITH f, count(c) AS change_counter RETURN f.name, change_counter ORDER BY change_counter DESC;
;; * bus factor (number of authors per file)
;;   requires clean-up of author nodes
;;   MATCH (f:File)<--(c:Commit)<--(a:Author) WITH f, count(a) AS busfactor WHERE busfactor < 1 RETURN f.name, busfactor LIMIT 10;
;; * testing quotient (how strictly are changes to main accompanied by changes to test?)
;;   could indicate either lacking test coverage or a rigidity hazard caused by
;;   the test suite that will make refactoring expensive
