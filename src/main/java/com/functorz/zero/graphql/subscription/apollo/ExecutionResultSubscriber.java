package com.functorz.zero.graphql.subscription.apollo;

import graphql.ExecutionResult;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Builder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ExecutionResultSubscriber implements Subscriber<ExecutionResult> {
  private Subscription subscription;
  private Consumer<ExecutionResult> onData;
  private Consumer<Throwable> onError;
  private Runnable onComplete;

  @Builder
  public ExecutionResultSubscriber(Consumer<ExecutionResult> onData,
                                   Consumer<Throwable> onError,
                                   Runnable onComplete) {
    this.onData = onData;
    this.onError = onError;
    this.onComplete = onComplete;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    this.subscription.request(1);
  }

  @Override
  public void onNext(ExecutionResult executionResult) {
    onData.accept(executionResult);
    subscription.request(1);
  }

  @Override
  public void onError(Throwable t) {
    onError.accept(t);
  }

  @Override
  public void onComplete() {
    onComplete.run();
  }

  public void cancel() {
    if (Objects.nonNull(subscription)) {
      subscription.cancel();
    }
  }
}
