package com.couchbase;

import com.couchbase.client.java.Bucket;

import java.io.IOException;
import java.text.ParseException;

import static com.couchbase.verifier.healthCheck;

public class loadme {

        public static void main(String[] args) throws IOException, ParseException {

            Bucket bucket = ConnectionManager.getConnection();
            Bucket bucket2 = ConnectionManager.getConnection2();

            int i = 0;
            int x = 20000;

            for (i=0; i<x; i++) {
                Boolean result = healthCheck(bucket, bucket2);
                if (result == false) {
                    System.out.println(i);
                    System.out.println(result);
                }
            }

            bucket.close();
            bucket2.close();
    }
}

/* This is the older loader let's move this out for now
    int numDocs = 1000000;
    final int MAX_RETRIES = 20000;
    final int RETRY_DELAY = 50;
    final int MAX_DELAY = 1000;
    JsonDocument doc = null;
    List<JsonDocument> docArray = new ArrayList<>();

    //Be sure to create a baseline document to use first
    JsonObject jsonBase = bucket.get("BASE").content();

            for (int i = 0; i < numDocs; i++) {

        // Create an id to use
        UUID theID = UUID.randomUUID();

        // Add the ID as an attribute to the document
        jsonBase.put("THIS_ID", i + "::" + theID.toString());
        // Create the json document
        doc = JsonDocument.create(theID.toString(), jsonBase);
        // Build the array of items to load
        docArray.add(i, doc);
        }

        long start = System.nanoTime();

        rx.Observable
        .from(docArray)
        .flatMap(doc1 -> {
        return bucket.async().insert(doc1)
        // do retry for each op individually to not fail the full batch
        .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
        .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
        })
        .toBlocking()
        .subscribe(doc2 -> {});

        long end = System.nanoTime();

        System.out.println("Bulk loading "+numDocs+" docs took: "+TimeUnit.NANOSECONDS.toSeconds(end-start)+"s.");
            */
