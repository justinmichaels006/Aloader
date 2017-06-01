package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestResource {

    public static void main(String[] args) throws IOException, ParseException {
        //Cluster cluster = CouchbaseCluster.create(config.getServers());
        //Bucket sharedBucket = cluster.openBucket(config.getBucket(), config.getPassword());
        Bucket bucket = ConnectionManager.getConnection2();

        AtomicLong statusPrinted = new AtomicLong(System.currentTimeMillis());
        ExecutorService pool = Executors.newFixedThreadPool(4);

        for (int thread = 1; thread <= 4; thread++) {
            final int threadId = thread;


            Runnable r = new Runnable() {
                public void run() {
                    //Bucket bucket;

                    /*if (config.isCouchbaseSharedThreadBucket()) {
                        bucket = sharedBucket;
                    } else {
                        sharedBucket.close();
                        bucket = cluster.openBucket(config.getBucket(), config.getPassword());
                    }*/

                    long counter = 0;
                    while (!pool.isShutdown() && counter <= 10) {
                        boolean failed = false;

                        try {
							/* Begin couchbase client method */
                            int maxFails = 5;
                            int again = 0;
                            AtomicInteger tri = new AtomicInteger(0);

                            JsonDocument status = null;
                            String uuid = UUID.randomUUID().toString();

                            JsonObject add = JsonObject.fromJson("{\"uuid\":\"" + uuid + "\", \"count\": " + counter + "}");

                            while (status == null && tri.incrementAndGet() < 4) {
                                try {
                                    failed = false;
                                    /*if (config.isCouchbaseUsePersistToOne() && config.isCouchbaseUseReplicateToOne()) {
                                        status = bucket.upsert(JsonDocument.create(uuid, add), PersistTo.ONE, ReplicateTo.ONE);
                                    } else if (config.isCouchbaseUsePersistToOne()) {
                                        status = bucket.upsert(JsonDocument.create(uuid, add), PersistTo.ONE);
                                    } else if (config.isCouchbaseUseReplicateToOne()) {
                                        status = bucket.upsert(JsonDocument.create(uuid, add), ReplicateTo.ONE);
                                    } else {
                                        status = bucket.upsert(JsonDocument.create(uuid, add));
                                    }*/
                                    status = bucket.upsert(JsonDocument.create(uuid, add));

                                    if (status == null) {
                                        failed = true;
                                        System.out.println("Failed save! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " status: null");
                                    } else {
                                        JsonDocument get = bucket.get(uuid);

                                        JsonObject content = get == null ? null : get.content();

                                        if (!Objects.equals(add, content)) {
                                            failed = true;
                                            System.out.println("Mismatched data! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " Add Value: " + add + " != Get Value: " + content);
                                        }
                                    }
                                } catch (Throwable e) {
                                    failed = true;
                                    System.out.println("Exception! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " Message: " + e.getClass().getName() + " " + e.getMessage());
                                    try {
                                        Thread.sleep(10);
                                    } catch (InterruptedException e1) {
                                        // ignore
                                    }
                                }
                            }
                            if (failed) {
                                again = again + 1;
                                if (again > maxFails) {
                                    System.out.println("Max Fails Exceeded " + maxFails + ": Thread: " + threadId + " Counter: " + counter);
                                    pool.shutdown();
                                }
                            }
							/* End couchbase client method */
                        } finally {
                            long now = System.currentTimeMillis();
                            long elapsed = now - statusPrinted.getAndSet(now);
                            System.out.println("Processed " + (counter - 1) + " total in " + elapsed + "ms for chunk of Thread: " + threadId);
                            }
                        }
                    }
                };
            pool.submit(r);
            };
        }
}
