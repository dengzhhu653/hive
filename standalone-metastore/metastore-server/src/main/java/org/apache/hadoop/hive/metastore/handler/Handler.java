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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.hadoop.hive.metastore.ExceptionHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

public abstract class Handler<Result> {
  private final ExceptionTransformer transformer;
  private final HandlerHookRunner hookRunner;

  private Handler(ExceptionTransformer transformer, HandlerHookRunner<Result> hookRunner) {
    this.transformer = transformer;
    this.hookRunner = hookRunner;
  }

  public Result run() throws TException {
    Result result = null;
    Exception ex = null;
    hookRunner.preRun();
    try {
      result = runInternal();
      return result;
    } catch (Exception e) {
      ex = e;
      throw transformer.transformException(e);
    } finally {
      hookRunner.postRun(ex, result);
    }
  }
  
  protected abstract Result runInternal() throws Exception;
  
  public interface ExceptionTransformer {
    TException transformException(Exception rawException) throws TException;
  }

  public interface HandlerHookRunner<T> {
    void preRun();
    void postRun(Exception ex, T result);
  }

  /** Rethrow {@link TException} as-is; otherwise wrap as {@link org.apache.hadoop.hive.metastore.api.MetaException}. */
  public static ExceptionTransformer defaultTException() {
    return e -> ExceptionHandler.handleException(e).defaultTException();
  }

  /**
   * Rethrow any exception whose type is listed in {@code declared}, then {@link #defaultTException()}.
   */
  @SafeVarargs
  public static ExceptionTransformer declared(Class<? extends TException>... declared) {
    return e -> {
      for (Class<? extends TException> type : declared) {
        if (type.isInstance(e)) {
          throw type.cast(e);
        }
      }
      return ExceptionHandler.handleException(e).defaultTException();
    };
  }

  public static ExceptionTransformer ioToMeta() {
    return e -> ExceptionHandler.handleException(e)
        .convertIfInstance(IOException.class, MetaException.class)
        .defaultTException();
  }

  public static ExceptionTransformer ioOrInterruptedToMeta() {
    return e -> ExceptionHandler.handleException(e)
        .convertIfInstance(IOException.class, MetaException.class)
        .convertIfInstance(InterruptedException.class, MetaException.class)
        .defaultTException();
  }

  public static ExceptionTransformer metaNoSuchIoToMeta() {
    return e -> ExceptionHandler.handleException(e)
        .throwIfInstance(MetaException.class, NoSuchObjectException.class)
        .convertIfInstance(IOException.class, MetaException.class)
        .defaultTException();
  }

  /** Run without {@code startFunction} / {@code endFunction} hooks. */
  public static <T> T runSilently(Callable<T> call) throws TException {
    return runSilently(defaultTException(), call);
  }

  public static <T> T runSilently(ExceptionTransformer transformer, Callable<T> call) throws TException {
    return new Builder<T>(transformer).handle(call).run();
  }

  public static <T> T runAudited(
      Runnable preRun,
      BiConsumer<T, Exception> postRun,
      Callable<T> call, ExceptionTransformer... transformer) throws TException {
    if (transformer == null || transformer.length == 0) {
      transformer = new ExceptionTransformer[] {defaultTException()};
    }
    return new Builder<T>(transformer[0]).preRun(preRun).postRun(postRun).handle(call).run();
  }

  public static <T> T runAuditedWithTx(
      RawStore ms,
      Runnable preRun,
      BiConsumer<T, Exception> postRun,
      Callable<T> call,
      ExceptionTransformer... transformer) throws TException {
    if (transformer == null || transformer.length == 0) {
      transformer = new ExceptionTransformer[] {defaultTException()};
    }
    Builder<T> builder = new Builder<T>(transformer[0])
        .preRun(preRun)
        .postRun(postRun)
        .withTx(ms)
        .handle(call);
    return builder.run();
  }

  public static class Builder<T> {
    private final ExceptionTransformer transformer;
    private Runnable preRun;
    private BiConsumer<T, Exception> postRun;
    private Callable<T> call;
    private boolean withTx;
    private RawStore rawStore;

    public Builder(ExceptionTransformer transformer) {
      this.transformer = transformer;
    }

    public Builder<T> preRun(Runnable r) {
      this.preRun = r;
      return this;
    }

    public Builder<T> postRun(BiConsumer<T, Exception> postRun) {
      this.postRun = postRun;
      return this;
    }

    public Builder<T> handle(Callable<T> call) {
      this.call = call;
      return this;
    }

    public Builder<T> withTx(RawStore rawStore) {
      this.rawStore = rawStore;
      this.withTx = true;
      return this;
    }

    public T run() throws TException {
      if (call == null
          || (preRun == null && postRun != null)
          || (preRun != null && postRun == null)) {
        throw new IllegalArgumentException();
      }
      AtomicBoolean txStatus = new AtomicBoolean(false);
      HandlerHookRunner<T> hookRunner = new HandlerHookRunner<T>() {
        @Override
        public void preRun() {
          if (preRun != null) {
            preRun.run();
          }
          if (withTx) {
            rawStore.openTransaction();
          }
        }
        @Override
        public void postRun(Exception ex, T result) {
          if (withTx && !txStatus.get()) {
            rawStore.rollbackTransaction();
          }
          if (postRun != null) {
            postRun.accept(result, ex);
          }
        }
      };
      Handler<T> handler = new Handler<>(transformer, hookRunner) {
        @Override
        protected T runInternal() throws Exception {
          T result = call.call();
          if (withTx) {
            txStatus.set(rawStore.commitTransaction());
          }
          return result;
        }
      };
      return handler.run();
    }
  }
}
