/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;

import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.Write;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.ExplicitlyWindowedFirestoreDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.FailedWritesException;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreWritePool.ContextAdapter;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * A collection of {@link org.apache.beam.sdk.transforms.DoFn DoFn}s for each of the supported write
 * RPC methods from the Cloud Firestore V1 API.
 */
final class FirestoreV1WriteFn {

  static final class BatchWriteFnWithSummary extends BaseBatchWriteFn<WriteSuccessSummary> {
    BatchWriteFnWithSummary(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, counterFactory);
    }

    @Override
    void handleWriteFailures(
        ContextAdapter<WriteSuccessSummary> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage) {
      throw new FailedWritesException(
          writeFailures.stream().map(KV::getKey).collect(Collectors.toList()));
    }

    @Override
    void handleWriteSummary(
        ContextAdapter<WriteSuccessSummary> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> summary,
        Runnable logMessage) {
      logMessage.run();
      context.output(summary.getKey(), timestamp, summary.getValue());
    }
  }

  static final class BatchWriteFnWithDeadLetterQueue extends BaseBatchWriteFn<WriteFailure> {
    BatchWriteFnWithDeadLetterQueue(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, counterFactory);
    }

    @Override
    void handleWriteFailures(
        ContextAdapter<WriteFailure> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage) {
      logMessage.run();
      for (KV<WriteFailure, BoundedWindow> kv : writeFailures) {
        context.output(kv.getKey(), timestamp, kv.getValue());
      }
    }

    @Override
    void handleWriteSummary(
        ContextAdapter<WriteFailure> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> tuple,
        Runnable logMessage) {
      logMessage.run();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link BatchWriteRequest}s.
   *
   * <p>Writes will be enqueued to be sent at a potentially later time when more writes are
   * available. This Fn attempts to maximize throughput while maintaining a high request success
   * rate.
   *
   * <p>All request pooling and quality-of-service is managed via the instance of {@link
   * FirestoreWritePool} associated with the lifecycle of this Fn.
   */
  abstract static class BaseBatchWriteFn<OutT> extends ExplicitlyWindowedFirestoreDoFn<Write, OutT>
      implements HasRpcAttemptContext {
    private final JodaClock clock;
    private final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    private final RpcQosOptions rpcQosOptions;
    private final CounterFactory counterFactory;
    private final V1FnRpcAttemptContext rpcAttemptContext;

    //  bundle scoped state
    @VisibleForTesting protected transient FirestoreWritePool<OutT> writePool;

    @SuppressWarnings(
        "initialization.fields.uninitialized") // allow transient fields to be managed by component
    // lifecycle
    BaseBatchWriteFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        CounterFactory counterFactory) {
      this.clock = clock;
      this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      this.rpcQosOptions = rpcQosOptions;
      this.counterFactory = counterFactory;
      this.rpcAttemptContext = V1FnRpcAttemptContext.BatchWrite;
    }

    @Override
    public Context getRpcAttemptContext() {
      return rpcAttemptContext;
    }

    @Override
    public final void populateDisplayData(DisplayData.Builder builder) {
      builder.include("rpcQosOptions", rpcQosOptions);
    }

    @Override
    public final void setup() {}

    @Override
    public final void startBundle(StartBundleContext c) {
      writePool =
          new FirestoreWritePool<>(
              clock,
              firestoreStatefulComponentFactory,
              rpcQosOptions,
              c.getPipelineOptions(),
              counterFactory,
              this::handleWriteSummary,
              this::handleWriteFailures);
    }

    /**
     * For each element extract and enqueue all writes from the commit. Then potentially flush any
     * previously and currently enqueued writes.
     *
     * <p>In order for writes to be enqueued the value of {@link BatchWriteRequest#getDatabase()}
     * must match exactly with the database name this instance is configured for via the provided
     * {@link org.apache.beam.sdk.options.PipelineOptions PipelineOptions}
     *
     * <p>{@inheritDoc}
     */
    @Override
    public void processElement(ProcessContext context, BoundedWindow window) throws Exception {
      @SuppressWarnings(
          "nullness") // error checker is configured to treat any method not explicitly annotated as
      // @Nullable as non-null, this includes Objects.requireNonNull
      Write write = requireNonNull(context.element(), "context.element() must be non null");
      writePool.write(write, window, ContextAdapter.forProcessContext(context));
    }

    /**
     * Attempt to flush any outstanding enqueued writes before cleaning up any bundle related state.
     * {@inheritDoc}
     */
    @SuppressWarnings("nullness") // allow clearing transient fields
    @Override
    public void finishBundle(FinishBundleContext context) throws Exception {
      writePool.close(ContextAdapter.forFinishBundleContext(context));
      writePool = null;
    }

    abstract void handleWriteFailures(
        ContextAdapter<OutT> context,
        Instant timestamp,
        List<KV<WriteFailure, BoundedWindow>> writeFailures,
        Runnable logMessage);

    abstract void handleWriteSummary(
        ContextAdapter<OutT> context,
        Instant timestamp,
        KV<WriteSuccessSummary, BoundedWindow> writeSummary,
        Runnable logMessage);
  }
}
