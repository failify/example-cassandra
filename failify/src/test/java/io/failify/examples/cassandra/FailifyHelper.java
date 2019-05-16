package io.failify.examples.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.PortType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.CommandResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeoutException;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfNodes) {
        String version = "3.11.4-SNAPSHOT";
        String dir = "apache-cassandra-" + version;
        Deployment.Builder builder = Deployment.builder("example-cassandra")
            .withService("cassandra")
                .applicationPath("../cassandra-3.11.4-build/" + dir + "-bin.tar.gz", "/cassandra", PathAttr.COMPRESSED)
                .startCommand("/cassandra/" + dir + "/bin/cassandra -R")
                .dockerImageName("failify/cassandra:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/cassandra/" + dir + "/logs")
                .applicationPath("config/cassandra.yaml", "/cassandra/" + dir + "/conf/cassandra.yaml")
                .tcpPort(9160).and();
        for (int i=1; i<=numOfNodes; i++) builder.withNode("n" + i, "cassandra").and();
        return builder.build();
    }

    public static CqlSession getCQLSession(FailifyRunner runner) {
        return CqlSession.builder().addContactPoint(new InetSocketAddress("n1", 9042))
                .withLocalDatacenter("dc1").build();
    }

    public static void waitForCluster(FailifyRunner runner) throws RuntimeEngineException {
        logger.info("Waiting for cluster to start up ...");
        String version = "3.11.4-SNAPSHOT";
        String dir = "apache-cassandra-" + version;
        CommandResults results;
        int attempt = 0;
        do {
            if (attempt++ >= 2) {
                throw new RuntimeEngineException("Timeout in waiting for cluster startup");
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeEngineException("waitForCluster sleep thread got interrupted");
            }
            results = runner.runtime().runCommandInNode("n1", "/cassandra/" + dir + "/bin/nodetool status");
            logger.info("nodetool results:\n{}", results.stdErr()+ results.stdOut());
        } while (results.exitCode() != 0);
    }
}
