FROM neo4j:4.1.3

# APOC and Neo4j versions need to match. See APOC install instructions
# https://neo4j-contrib.github.io/neo4j-apoc-procedures/
RUN bash -c "mkdir /plugins \
             && pushd /plugins \
             && wget https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/4.1.0.0/apoc-4.1.0.0-all.jar \
             && popd"
