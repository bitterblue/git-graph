(ns git-graph.core
  (:require [clj-jgit
             [porcelain :as jgit]
             [querying :as jgit-query]]
            [clojure.core.async :as async :refer [<!!]]
            [clojurewerkz.neocons.bolt :as neobolt]))

(defn- load-repo [path]
  (-> (jgit/discover-repo path)
      (jgit/load-repo)))

(defn commits
  "Returns a seq of commit-infos for all commits in the history of the current branch."
  [repo]
  (map (partial jgit-query/commit-info repo) (jgit/git-log repo)))

(defn load-commits-stmt [cs]
  (let [commit-properties (fn [c] {:id      (:id c)
                                   :time    (.getTime (:time c))
                                   :message (:message c)})]
    ["UNWIND {commits} AS commit
      MERGE (c:Commit {id: commit.id}) SET c = commit;"
     {:commits (mapv commit-properties cs)}]))

(defn load-authorships-stmt [cs]
  (let [author      (fn [c] {:name (:author c) :email (:email c)})
        authorships (->> cs
                         (group-by author)
                         (reduce-kv (fn [v a cs]
                                      (conj v (assoc a :commits (mapv :id cs))))
                                    []))]
    ["UNWIND {authorships} AS author
      MERGE (a {name: author.name, email: author.email})
      SET a :Author
      WITH a, author.commits AS hashes
      UNWIND hashes AS hash
      MATCH (c:Commit {id: hash})
      WITH a, c
      CREATE UNIQUE (a)-[:AUTHORED]->(c);"
     {:authorships authorships}]))

;; TODO: not parallelizable with authorship, so why not fuse them to a single statement?
(defn load-commissions-stmt [cs]
  (let [committer   (fn [c] {:name  (-> c :raw .getCommitterIdent .getName)
                             :email (-> c :raw .getCommitterIdent .getEmailAddress)})
        commissions (->> cs
                         (group-by committer)
                         (reduce-kv (fn [v a cs]
                                      (conj v (assoc a :commits (mapv :id cs))))
                                    []))]
    ["UNWIND {commissions} AS committer
      MERGE (a {name: committer.name, email: committer.email})
      SET a :Committer
      WITH a, committer.commits AS hashes
      UNWIND hashes AS hash
      MATCH (c:Commit {id: hash})
      WITH a, c
      CREATE UNIQUE (a)-[:COMMITTED]->(c);"
     {:commissions commissions}]))

(defn load-changesets-stmt [cs]
  (let [change-sets (mapv (fn [c] {:id    (:id c)
                                   :files (for [[f a] (:changed_files c)]
                                            {:name f :action (name a)})})
                          cs)]
    ["UNWIND {changesets} AS changeset
      MATCH (c:Commit {id: changeset.id})
      WITH c, changeset.files AS files
      UNWIND files AS file
      MERGE (f:File {name: file.name})
      MERGE (c)-[:CHANGES {action: file.action}]->(f);"
     {:changesets change-sets}]))

(defn load-parent-relations-stmt [cs]
  (let [relations (mapv (fn [c] {:id      (:id c)
                                 :parents (mapv #(-> % .getName str)
                                                (-> c :raw .getParents))})
                        cs)]
    ["UNWIND {relations} AS relation
      MATCH (a:Commit {id: relation.id}), (p:Commit)
      WHERE p.id IN relation.parents
      CREATE UNIQUE (a)-[:HAS_PARENT]->(p);"
     {:relations relations}]))

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
   (let [params (clojure.walk/stringify-keys params)]
     (try
       (neobolt/query session qry params)
       (catch Exception e
         (println "Exception when running query: " qry " with param map " params "!")
         (throw e))))))

(defn run-statements! [conn stmts]
  (with-open [session (neobolt/create-session conn)]
    (doseq [[qry params] stmts]
      (run-query! session qry params))))

(def parallelism 4)
(def buffer-size (* 2 parallelism))

(defn nodes-stream [repo]
  (let [commit-ch    (async/chan buffer-size)
        commit-mult  (async/mult commit-ch)
        commit-nodes (async/tap commit-mult
                                (async/chan buffer-size
                                            (map (fn [c]
                                                   {:type    :commit
                                                    :id      (:id c)
                                                    :time    (.getTime (:time c))
                                                    :message (:message c)}))))
        author-nodes (async/tap commit-mult
                                (async/chan buffer-size
                                            (comp (mapcat (fn [c]
                                                            [{:type  :author
                                                              :name  (:author c)
                                                              :email (:email c)}
                                                             {:type  :author
                                                              :name  (-> c :raw .getCommitterIdent .getName)
                                                              :email (-> c :raw .getCommitterIdent .getEmailAddress)}]))
                                                  (distinct))))
        file-nodes   (async/tap commit-mult
                                (async/chan buffer-size
                                            (comp (mapcat (fn [c]
                                                            (for [[f a] (:changed_files c)]
                                                              {:type :file
                                                               :name f})))
                                                  (distinct))))]
    (async/onto-chan commit-ch (commits repo))
    (async/merge [commit-nodes author-nodes file-nodes] buffer-size)))

(defn relations-stream [repo]
  (let [commit-ch   (async/chan buffer-size)
        commit-mult (async/mult commit-ch)
        parent-rels (async/tap commit-mult
                               (async/chan buffer-size
                                           (map (fn [c]
                                                  {:type    :parent
                                                   :id      (:id c)
                                                   :message (mapv #(-> % .getName str)
                                                                  (-> c :raw .getParents))}))))
        authorships (async/tap commit-mult
                               (async/chan buffer-size
                                           (mapcat (fn [c]
                                                     [{:type  :authored
                                                       :id    (:id c)
                                                       :name  (:author c)
                                                       :email (:email c)}
                                                      {:type  :committed
                                                       :id    (:id c)
                                                       :name  (-> c :raw .getCommitterIdent .getName)
                                                       :email (-> c :raw .getCommitterIdent .getEmailAddress)}]))))
        changesets (async/tap commit-mult
                              (async/chan buffer-size
                                          (mapcat (fn [c]
                                                    [{:type :changed-files
                                                      :id (:id c)
                                                      :files (into []
                                                                   (for [[f a] (:changed_files c)]
                                                                     {:name f :action (name a)}))}]))))]
    (async/onto-chan commit-ch (commits repo))
    (async/merge [parent-rels authorships changesets] buffer-size)))

(defmulti load-stmt :type)
(defmethod load-stmt :commit [node]
  ["CREATE (c:Commit {id: {commit}.id}) SET c = {commit};"
   {:commit (dissoc node :type)}])
(defmethod load-stmt :author [node]
  ["CREATE (a {name: {author}.name, email: {author}.email})
    SET a = {author}
    SET a :Author;"
   {:author (dissoc node :type)}])
(defmethod load-stmt :file [node]
  ["CREATE (f:File {name: {file}.name}) SET f = {file};"
   {:file (dissoc node :type)}])
(defmethod load-stmt :parent [rel]
  ["UNWIND {relations} AS relation
    MATCH (a:Commit {id: relation.id}), (p:Commit)
    WHERE p.id IN relation.parents
    CREATE UNIQUE (a)-[:HAS_PARENT]->(p);"
   {:relations (dissoc rel :type)}])
(defmethod load-stmt :authored [rel]
  ["MATCH (a:Author {name: {rel}.name, email: {rel}.email}), (c:Commit {id: {rel}.id})
    CREATE UNIQUE (a)-[:AUTHORED]->(c);"
   {:rel (dissoc rel :type)}])
(defmethod load-stmt :committed [rel]
  ["MATCH (a:Author {name: {rel}.name, email: {rel}.email}), (c:Commit {id: {rel}.id})
    CREATE UNIQUE (a)-[:COMMITTED]->(c);"
   {:rel (dissoc rel :type)}])
(defmethod load-stmt :changed-files [rel]
  ["MATCH (c:Commit {id: {changeset}.id})
    WITH c, {changeset}.files AS files
    UNWIND files AS file
    MERGE (f:File {name: file.name})
    MERGE (c)-[:CHANGES {action: file.action}]->(f);"
   {:changeset (dissoc rel :type)}])

;; TODO: include rename detection (jgit's RenameDetector?)

(defn import-graph [repo]
  (with-open [conn (neobolt/connect "bolt://localhost")]
    (let [make-loader (fn [ch conn]
                        (async/thread
                          (with-open [session (neobolt/create-session conn)]
                            (loop []
                              (when-let [[qry params] (async/<!! ch)]
                                (run-query! session qry params)
                                (recur))))))
          loader-ch   (async/chan buffer-size)
          loaders     (into [] (repeat parallelism (make-loader loader-ch conn)))]

      (let [nodes-ch (nodes-stream repo)
            rels-ch  (relations-stream repo)]
        (<!! (,,,)))

      ;; wait for loaders to finish
      (async/<!! (async/map vector loaders)))
    ))

(defn import-graph-serial [repo]
  (with-open [conn (neobolt/connect "bolt://localhost")]

    (println "Loading git graph ...")
    (run-statements! conn [(load-commits-stmt (commits repo))])

    (run-statements! conn [(load-authorships-stmt (commits repo))])
    (run-statements! conn [(load-commissions-stmt (commits repo))])
    (run-statements! conn [(load-changesets-stmt (commits repo))])
    (run-statements! conn [(load-parent-relations-stmt (commits repo))])

    (println "Post-processing the graph ...")
    (run-statements! conn [label-merge-commits-stmt
                           label-test-files-stmt
                                        ; merge-authors-with-same-name-stmt
                           ])))


;; analyses

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
                      // AND NOT a:Test AND NOT b:Test
                      // AND a.name ENDS WITH '.java' AND b.name ENDS WITH '.java'
                      AND id(a) < id(b)
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
