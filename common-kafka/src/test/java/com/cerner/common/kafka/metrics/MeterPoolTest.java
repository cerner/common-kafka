package com.cerner.common.kafka.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link MeterPool}.
 */
public class MeterPoolTest {

    @Before
    public void clearMetrics() {
        MetricsRegistry metricsRegistry = Metrics.defaultRegistry();
        for (MetricName metric : metricsRegistry.allMetrics().keySet()) {
            metricsRegistry.removeMetric(metric);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullClass() {
        new MeterPool(null, "name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullName() {
        new MeterPool(Object.class, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getMeterNullScope() {
        new MeterPool(Object.class, "name").getMeter(null);
    }

    @Test
    public void getMeterSameScope() {
        MeterPool pool = new MeterPool(Object.class, "name");

        Meter meter1 = pool.getMeter("scope");
        Meter meter2 = pool.getMeter("scope");

        assertSame(meter1, meter2);
    }

    @Test
    public void getMeterDifferentScope() {
        MeterPool pool = new MeterPool(Object.class, "name");

        Meter meter1 = pool.getMeter("scope1");
        Meter meter2 = pool.getMeter("scope2");

        assertThat(meter1, is(not(equalTo(meter2))));
    }
}
