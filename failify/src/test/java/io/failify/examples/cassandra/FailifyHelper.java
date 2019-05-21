package io.failify.examples.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.StringJoiner;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfNodes) {
        String version = "3.11.4-SNAPSHOT"; // this can be dynamically generated from maven metadata
        String dir = "apache-cassandra-" + version;
        StringJoiner seeds = new StringJoiner(",");
        for (int i=1; i<=numOfNodes; i++) seeds.add("n" + i);
        Deployment.Builder builder = Deployment.builder("example-cassandra")
            .withService("cassandra")
                .applicationPath("../cassandra-3.11.4-build/" + dir + "-bin.tar.gz", "/cassandra", PathAttr.COMPRESSED)
                .workDir("/cassandra/" + dir).startCommand("bin/cassandra -R")
                .dockerImageName("failify/cassandra:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/cassandra/" + dir + "/logs")
                .applicationPath("config/cassandra.yaml", "/cassandra/" + dir + "/conf/cassandra.yaml",
                        new HashMap<String, String>() {{ put("CASSANDRA_SEEDS", seeds.toString());}})
                .applicationPath("config/cassandra-env.sh", "/cassandra/" + dir + "/conf/cassandra-env.sh")
                .ulimit(ULimit.MEMLOCK, -1).and();
        for (int i=1; i<=numOfNodes; i++) builder.withNode("n" + i, "cassandra").and();
        return builder.build();
    }

    public static CqlSession getCQLSession(FailifyRunner runner, int numOfContactPoints) {
        CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter("dc1");
        for (int i=1; i<=numOfContactPoints; i++) builder.addContactPoint(new InetSocketAddress("n" + i, 9042));
        return builder.build();
    }

    public static void waitForCluster(FailifyRunner runner, int numOfNodes) throws RuntimeEngineException {
        logger.info("Waiting for cluster to start up ...");


        boolean started = false;
        int attempt = 0;
        do {
            if (attempt++ >= 15) {
                throw new RuntimeEngineException("Timeout in waiting for cluster startup");
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeEngineException("waitForCluster sleep thread got interrupted");
            }

            try {
                getCQLSession(runner, numOfNodes);
                started = true;
            } catch (Exception e) {
                // possible place to debug log something
            }
        } while (!started);
    }
}
