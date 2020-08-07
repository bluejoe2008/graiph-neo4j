package org.neo4j.server.configuration;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.neo4j.kernel.configuration.Config;

public interface ApplicationContextEnhancer {
    void enhance(ServletContextHandler handler, Config config);
}
