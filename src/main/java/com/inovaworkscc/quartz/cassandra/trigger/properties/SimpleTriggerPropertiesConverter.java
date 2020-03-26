package com.inovaworkscc.quartz.cassandra.trigger.properties;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerPropertiesConverter;
import java.util.HashMap;
import org.quartz.SimpleTrigger;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class SimpleTriggerPropertiesConverter extends TriggerPropertiesConverter {

    public static final String TRIGGER_REPEAT_COUNT = "repeatCount";
    public static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
    public static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof SimpleTriggerImpl)
                && !((SimpleTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public HashMap<String, Object> injectExtraPropertiesForInsert(OperableTrigger trigger, HashMap<String, Object> original) {
        SimpleTrigger t = (SimpleTrigger) trigger;

        HashMap<String, Object> neu = new HashMap<> (original);
        neu.put(TRIGGER_REPEAT_COUNT, t.getRepeatCount());
        neu.put(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval());
        neu.put(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
        
        return neu;
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Row stored) {

        SimpleTriggerImpl t = (SimpleTriggerImpl) trigger;

        if (!stored.isNull(TRIGGER_REPEAT_COUNT)) {
            
            int repeatCount = stored.getInt(TRIGGER_REPEAT_COUNT);

            t.setRepeatCount(repeatCount);
        }
        
        if (!stored.isNull(TRIGGER_REPEAT_INTERVAL)) {
            
            long repeatInterval = stored.getLong(TRIGGER_REPEAT_INTERVAL);

            t.setRepeatInterval(repeatInterval);
        }
        if (!stored.isNull(TRIGGER_TIMES_TRIGGERED)) {
            
            int timesTriggered = stored.getInt(TRIGGER_TIMES_TRIGGERED);

            t.setTimesTriggered(timesTriggered);
        }
    }
}
