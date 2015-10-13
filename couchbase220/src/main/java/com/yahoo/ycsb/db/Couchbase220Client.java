package com.yahoo.ycsb.db;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.*;
import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by kruthar on 2/4/15.
 */
public class Couchbase220Client extends DB {
    public static final int OK = 0;
    public static final int FAIL = 1;

    private CouchbaseClusterManager manager;
    private Bucket bucket;
    private final Logger log = LoggerFactory.getLogger(getClass());


    public Couchbase220Client() throws DBException {}

    public void init() throws DBException {
        Properties props = getProperties();
        manager = CouchbaseClusterManager.getInstance(props);
        bucket = manager.getBucket();
    }

    public void cleanup() throws DBException {
        manager.disconnect();
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String qk = getQualifiedKey(table, key);
        try {
            JsonDocument document = bucket.get(qk);
            if (document != null) {
                JsonObject content = document.content();
                if (fields == null) {
                    for (String field : content.getNames()) {
                        result.put(field, new StringByteIterator(content.get(field).toString()));
                    }
                } else {
                    for (String field : fields) {
                        // TODO: Is this conversion through StringByteIterator efficient?
                        result.put(field, new StringByteIterator(document.content().get(field).toString()));
                    }
                }
                return OK;
            }
        } catch (Exception e) {
            log.error("Could not read document at " + qk, e);
        }
        return FAIL;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return FAIL;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        String qk = getQualifiedKey(table, key);
        try {
//            JsonDocument document = bucket.get(qk);
//            if (document != null) {
//                for (Map.Entry entry : values.entrySet()) {
//                    document.content().put(entry.getKey().toString(), entry.getValue().toString());
//                }
//                bucket.upsert(document);
//                return OK;
//            }
            JsonObject obj = JsonObject.create();
            for (Map.Entry entry : values.entrySet()) {
                obj.put(entry.getKey().toString(), entry.getValue().toString());
            }
            JsonDocument document = JsonDocument.create(qk, obj);
            bucket.upsert(document);
            return OK;
        } catch (Exception e) {
            log.error("Could not update document at " + qk, e);
        }
        return FAIL;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        String qk = getQualifiedKey(table, key);
        try {
            JsonObject object = JsonObject.empty();
            for (Map.Entry entry : values.entrySet()) {
                object.put(entry.getKey().toString(), entry.getValue().toString());
            }
            JsonDocument document = JsonDocument.create(qk, object);
            bucket.insert(document);
        } catch (Exception e) {
            log.error("Could not insert " + qk, e);
            return FAIL;
        }
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        String qk = getQualifiedKey(table, key);
        try {
            bucket.remove(qk);
        } catch (Exception e) {
            log.error("Could not delete value for key " + qk, e);
            return FAIL;
        }
        return OK;
    }

    private String getQualifiedKey(String table, String key) {
        return table + "-" + key;
    }
}
