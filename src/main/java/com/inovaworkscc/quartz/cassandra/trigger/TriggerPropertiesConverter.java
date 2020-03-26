package com.inovaworkscc.quartz.cassandra.trigger;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.trigger.properties.CalendarIntervalTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.CronTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.DailyTimeIntervalTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.SimpleTriggerPropertiesConverter;

import org.quartz.spi.OperableTrigger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Converts trigger type specific properties.
 */
public abstract class TriggerPropertiesConverter {

    private static final List<TriggerPropertiesConverter> propertiesConverters = Arrays.asList(
            new SimpleTriggerPropertiesConverter(),
            new CalendarIntervalTriggerPropertiesConverter(),
            new CronTriggerPropertiesConverter(),
            new DailyTimeIntervalTriggerPropertiesConverter());

    /**
     * Returns properties converter for given trigger or null when not found.
     * @param trigger    a trigger instance
     * @return converter or null
     */
    public static TriggerPropertiesConverter getConverterFor(OperableTrigger trigger) {
        for (TriggerPropertiesConverter converter : propertiesConverters) {
            if (converter.canHandle(trigger)) {
                return converter;
            }
        }
        return null;
    }

    protected abstract boolean canHandle(OperableTrigger trigger);

    public abstract HashMap<String, Object> injectExtraPropertiesForInsert(OperableTrigger trigger, HashMap<String, Object> original);

    public abstract void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Row stored);
}
