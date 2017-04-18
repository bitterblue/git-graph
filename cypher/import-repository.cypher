// Import a git graph from CSV files

// constraints and indices
CREATE CONSTRAINT ON (c:Commit) ASSERT c.id IS UNIQUE;
CREATE INDEX ON :Author(name);
CREATE INDEX ON :Committer(name);
CREATE CONSTRAINT ON (f:File) ASSERT f.name IS UNIQUE;


// commits
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///commits.csv" AS commit
MERGE (c:Commit {id: commit.id})
ON CREATE SET c.time = toInt(commit.time),
              c.message = commit.message;

// authors and their relations to commits
LOAD CSV WITH HEADERS FROM "file:///authorship.csv" AS author
MERGE (a {name: author.name, email: author.email})
ON CREATE SET a :Author
WITH a, split(author.commits, ";") AS hashes
UNWIND hashes AS hash
MATCH (c:Commit {id: hash})
MERGE (a)-[:AUTHORED]->(c);

// committers and their relations to commits
LOAD CSV WITH HEADERS FROM "file:///commissions.csv" AS committer
MERGE (a {name: committer.name, email: committer.email})
ON CREATE SET a :Committer
WITH a, split(committer.commits, ";") AS hashes
UNWIND hashes AS hash
MATCH (c:Commit {id: hash})
MERGE (a)-[:COMMITTED]->(c);

// changesets for each commit
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///changesets.csv" AS file
MATCH (c:Commit {id: file.commit})
MERGE (f:File {name: file.name})
MERGE (c)-[:CHANGES {action: file.action}]->(f);

// ancestry: parent relationship for every commit
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///ancestry.csv" AS commit
WITH commit.id AS id, split(commit.parents, ";") AS parents
MATCH (c:Commit {id: id}), (p:Commit)
WHERE p.id IN parents
MERGE (c)-[:HAS_PARENT]->(p);

// Post-Processing and Clean-Up
//
// label merge commits
MATCH (c:Commit)-[p:HAS_PARENT]->()
WITH c, count(p) AS parent_count
WHERE parent_count > 1
SET c :Merge;

// label test files (all files in a "test" folder)
MATCH (f:File)
WHERE f.name CONTAINS '/test/'
SET f :Test;

// mark files as deleted, iff they were deleted by the last commit that touched them
MATCH (c:Commit)-->(f:File)
WITH f, max(c.time) AS last_modified
MATCH (c:Commit)-[r:CHANGES]->(f)
WHERE c.time = last_modified
SET f.deleted = (r.action = "delete");

// merge author nodes with same name
// MATCH (a:Author)
// WITH a.name AS name, collect(a) AS authors, collect(a.email) AS emails, count(a) AS hits
// WHERE hits > 1
// CALL apoc.refactor.mergeNodes(authors) YIELD node
// SET node.alternate_emails = emails;

// TODO: merge nodes with same email as well?

