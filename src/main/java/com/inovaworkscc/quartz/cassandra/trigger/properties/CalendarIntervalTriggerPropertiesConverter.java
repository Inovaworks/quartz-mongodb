package com.inovaworkscc.quartz.cassandra.trigger.properties;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerPropertiesConverter;
import java.util.HashMap;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CalendarIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {

    public static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
    public static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
    public static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof CalendarIntervalTriggerImpl)
                && !((CalendarIntervalTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public HashMap<String, Object> injectExtraPropertiesForInsert(OperableTrigger trigger, HashMap<String, Object> original) {
        CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

        HashMap<String, Object> neu = new HashMap<> (original);
        neu.put(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name());
        neu.put(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval());
        neu.put(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
        
        return neu;
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Row stored) {
        
        CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

        String repeatIntervalUnit = stored.getString(TRIGGER_REPEAT_INTERVAL_UNIT);
        if (repeatIntervalUnit != null) {
            t.setRepeatIntervalUnit(IntervalUnit.valueOf(repeatIntervalUnit));
        }
        if (!stored.isNull(TRIGGER_REPEAT_INTERVAL)) {
            
            int repeatInterval = stored.getInt(TRIGGER_REPEAT_INTERVAL);

            t.setRepeatInterval(repeatInterval);
        }
        if (!stored.isNull(TRIGGER_TIMES_TRIGGERED)) {
            
            int timesTriggered = stored.getInt(TRIGGER_TIMES_TRIGGERED);

            t.setTimesTriggered(timesTriggered);
        }

    }
}
