package com.inovaworkscc.quartz.cassandra.trigger.properties;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerPropertiesConverter;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.text.ParseException;
import java.util.HashMap;
import java.util.TimeZone;

public class CronTriggerPropertiesConverter extends TriggerPropertiesConverter {

    public static final String TRIGGER_CRON_EXPRESSION = "cronExpression";
    public static final String TRIGGER_TIMEZONE = "timezone";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof CronTriggerImpl)
                && !((CronTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public HashMap<String, Object> injectExtraPropertiesForInsert(OperableTrigger trigger, HashMap<String, Object> original) {
        CronTrigger t = (CronTrigger) trigger;

        HashMap<String, Object> neu = new HashMap<> (original);
        neu.put(TRIGGER_CRON_EXPRESSION, t.getCronExpression());
        neu.put(TRIGGER_TIMEZONE, t.getTimeZone().getID());
        
        return neu;
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Row stored) {

        CronTriggerImpl t = (CronTriggerImpl) trigger;

        String expression = stored.getString(TRIGGER_CRON_EXPRESSION);
        if (expression != null) {
            try {
                t.setCronExpression(new CronExpression(expression));
            } catch (ParseException e) {
                // no good handling strategy and
                // checked exceptions route sucks just as much.
            }
        }
        String tz = stored.getString(TRIGGER_TIMEZONE);
        if (tz != null) {
            t.setTimeZone(TimeZone.getTimeZone(tz));
        }
    }
}
