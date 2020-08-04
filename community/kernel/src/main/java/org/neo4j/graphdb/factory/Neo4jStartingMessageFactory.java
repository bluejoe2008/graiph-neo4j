package org.neo4j.graphdb.factory;

import org.neo4j.kernel.internal.Version;

public class Neo4jStartingMessageFactory implements StartingMessageFactory {
    @Override
    public String getMessage() {
        return "======== Neo4j " + Version.getNeo4jVersion() + " ========";
    }
}
