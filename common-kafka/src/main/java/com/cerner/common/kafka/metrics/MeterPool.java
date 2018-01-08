package com.cerner.common.kafka.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A pool of {@link Meter meter} objects.
 * <p>
 * This class is thread-safe.
 * </p>
 *
 * @author A. Olson
 */
public class MeterPool {

    private final Class<?> clazz;
    private final String name;
    private final ConcurrentMap<String, Meter> meters;

    /**
     * Constructs a pool for meters.
     *
     * @param clazz The class that owns the meter.
     * @param name The name of the meter.
     *
     * @throws IllegalArgumentException
     *             if any argument is {@code null}.
     */
    public MeterPool(Class<?> clazz, String name) {
        if (clazz == null) {
            throw new IllegalArgumentException("class cannot be null");
        }
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
        }
        this.clazz = clazz;
        this.name = name;
        this.meters = new ConcurrentHashMap<String, Meter>();
    }

    /**
     * Returns the meter corresponding to the given scope.
     *
     * @param scope The scope of the meter to return.
     *
     * @return a {@link Meter} with the given scope.
     *
     * @throws IllegalArgumentException
     *             if {@code scope} is {@code null}.
     */
    public Meter getMeter(String scope) {
        if (scope == null) {
            throw new IllegalArgumentException("scope cannot be null");
        }
        Meter meter = meters.get(scope);
        if (meter == null) {
            meter = Metrics.newMeter(clazz, name, scope, name, TimeUnit.SECONDS);
            Meter existing = meters.putIfAbsent(scope, meter);
            if (existing != null) {
                meter = existing;
            }
        }
        return meter;
    }
}
