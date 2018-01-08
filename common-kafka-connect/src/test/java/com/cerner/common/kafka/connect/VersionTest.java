package com.cerner.common.kafka.connect;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

public class VersionTest {

    @Test
    public void getVersion() {
        // We can't make a lot of assertions without writing/using the same code in getVersion so just verify it is
        // not any of the bad cases
        assertThat(Version.getVersion(), is(not("unknown")));
        assertThat(Version.getVersion(), is(not(nullValue())));
        assertThat(Version.getVersion(), is(not("")));
    }
}
