package io.github.resilience4j.ratpack.timelimiter;

import io.github.resilience4j.core.lang.Nullable;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratpack.internal.AbstractTransformer;
import io.github.resilience4j.timelimiter.TimeLimiter;
import ratpack.exec.Downstream;
import ratpack.exec.Upstream;
import ratpack.func.Function;

public class TimeLimiterTransformer<T> extends AbstractTransformer<T> {

    private final TimeLimiter timeLimiter;

    private TimeLimiterTransformer(TimeLimiter timeLimiter) {
        this.timeLimiter = timeLimiter;
    }

    /**
     * Create a new transformer that can be applied to the {@link ratpack.exec.Promise#transform(Function)} method.
     * The Promised value will pass through the timelimiter, potentially causing it to timeout.
     *
     * @param timeLimiter the timelimiter to use
     * @param <T> the type of object
     * @return the transformer
     */
    public static <T> TimeLimiterTransformer<T> of(TimeLimiter timeLimiter) {
        return new TimeLimiterTransformer<>(timeLimiter);
    }

    /**
     * Set a recovery function that will execute when the retry limit is exceeded.
     *
     * @param recoverer the recovery function
     * @return the transformer
     */
    public TimeLimiterTransformer<T> recover(@Nullable Function<Throwable, ? extends T> recoverer) {
        this.recoverer = recoverer;
        return this;
    }

    @Override
    public Upstream<T> apply(Upstream<? extends T> upstream) throws Exception {
        return down -> {
            boolean permission = rateLimiter.acquirePermission();
            if (Thread.interrupted()) {
                throw new IllegalStateException("Thread was interrupted during permission wait");
            }
            if (!permission) {
                Throwable t = new RequestNotPermitted(rateLimiter);
                if (recoverer != null) {
                    down.success(recoverer.apply(t));
                } else {
                    down.error(t);
                }
            } else {
                upstream.connect(new Downstream<T>() {

                    @Override
                    public void success(T value) {
                        down.success(value);
                    }

                    @Override
                    public void error(Throwable throwable) {
                        down.error(throwable);
                    }

                    @Override
                    public void complete() {
                        down.complete();
                    }
                });
            }
        };    }

}
