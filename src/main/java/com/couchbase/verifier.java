package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.handler.timeout.TimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class verifier {

    public static boolean healthCheck(Bucket primaryBucket, Bucket secondaryBucket) {
        try {
            primaryBucket
                    .async()
                    .get("dummyValue")
                    .timeout(5000, TimeUnit.MILLISECONDS)
                    .retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                    .retryWhen(RetryBuilder.anyOf(TemporaryFailureException.class)
                            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                    .retryWhen(RetryBuilder.anyOf(TimeoutException.class)
                            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                    .toBlocking()
                    .single();
        } catch (final NoSuchElementException ex) {
        } catch (final Exception ex) {
            System.out.println("Error primary bucket" + ex);
            try {
                secondaryBucket
                        .async()
                        .get("dummyValue")
                        .timeout(5000,TimeUnit.MILLISECONDS)
                        .retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                                .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                        .retryWhen(RetryBuilder.anyOf(TemporaryFailureException.class)
                                .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                        .retryWhen(RetryBuilder.anyOf(TimeoutException.class)
                                .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(3).build())
                        .toBlocking()
                        .single();
            } catch (final NoSuchElementException e) {
            } catch (final Exception e) {
                System.out.println("Error secondary bucket" + e);
            }
        }

        return true;
    }
}
