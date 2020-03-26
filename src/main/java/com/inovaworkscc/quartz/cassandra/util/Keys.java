package com.inovaworkscc.quartz.cassandra.util;

import com.datastax.driver.core.Row;
import org.quartz.JobKey;
import org.quartz.TriggerKey;

public class Keys {

    public enum LockType { t, j }

    public static final String LOCK_TYPE = "type";
    public static final String KEY_NAME = "keyName";
    public static final String KEY_GROUP = "keyGroup";

    public static JobKey toJobKey(Row dbo) {
        return new JobKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }
    
    public static TriggerKey toTriggerKey(Row dbo) {
        return new TriggerKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }
}
