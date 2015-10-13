package com.yahoo.ycsb.db;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.yahoo.ycsb.DBException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by kruthar on 9/19/15.
 */
public class CouchbaseClusterManager {
    public static final String URL_PROPERTY = "couchbase.url";
    public static final String BUCKET_PROPERTY = "couchbase.bucket";
    public static final String PASSWORD_PROPERTY = "couchbase.password";

    private static CouchbaseClusterManager instance = null;
    private Cluster cluster = null;
    private Bucket bucket = null;
    private static int openUsers = 0;

    protected CouchbaseClusterManager(Properties props) throws DBException {
        List<String> nodes = Arrays.asList(props.getProperty(URL_PROPERTY, "").split(","));
        String bucket_name = props.getProperty(BUCKET_PROPERTY, "default");
        String bucket_password = props.getProperty(PASSWORD_PROPERTY, "");

        try {
            cluster = CouchbaseCluster.create(nodes);
            bucket = cluster.openBucket(bucket_name, bucket_password);
        } catch (Exception e) {
            throw new DBException("Could not connect to Couchbase Cluster.", e);
        }
    }

    public static synchronized CouchbaseClusterManager getInstance(Properties properties) throws DBException {
        if (instance == null) {
            instance = new CouchbaseClusterManager(properties);
        }

        openUsers++;
        return instance;
    }

    public Bucket getBucket() {
        return bucket;
    }

    public void disconnect() {
        openUsers--;
        if (openUsers <= 0) {
            cluster.disconnect();
            instance = null;
        }
    }
}
