FROM neo4j:3.1.3

RUN bash -c "apk update \
             && apk add ca-certificates wget \
             && update-ca-certificates \
             && mkdir /plugins \
             && pushd /plugins \
             && wget https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/3.1.3.6/apoc-3.1.3.6-all.jar \
             && popd"

