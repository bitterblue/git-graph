// Import a git graph from CSV files

// constraints and indices
CREATE CONSTRAINT ON (c:Commit) ASSERT c.id IS UNIQUE;

// commits
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///commits.csv" AS commit
MERGE (c:Commit {id: commit.id})
SET c = commit;

// authors and their relations to commits
LOAD CSV WITH HEADERS FROM "file:///authorship.csv" AS author
MERGE (a {name: author.name, email: author.email})
SET a :Author
WITH a, split(author.commits, ";") AS hashes
UNWIND hashes AS hash
MATCH (c:Commit {id: hash})
CREATE UNIQUE (a)-[:AUTHORED]->(c);

// committers and their relations to commits
LOAD CSV WITH HEADERS FROM "file:///commissions.csv" AS committer
MERGE (a {name: committer.name, email: committer.email})
SET a :Committer
WITH a, split(committer.commits, ";") AS hashes
UNWIND hashes AS hash
MATCH (c:Commit {id: hash})
CREATE UNIQUE (a)-[:COMMITTED]->(c);

// changesets for each commit
USING PERIODIC COMMIT
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

