package com.inovaworkscc.quartz.cassandra.trigger.properties;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerPropertiesConverter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.DateBuilder;

public class DailyTimeIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {

    public static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
    public static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
    public static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";
    public static final String TRIGGER_START_TIME_OF_DAY = "startTimeOfDay";
    public static final String TRIGGER_END_TIME_OF_DAY = "endTimeOfDay";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof DailyTimeIntervalTrigger)
                && !((DailyTimeIntervalTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public HashMap<String, Object> injectExtraPropertiesForInsert(OperableTrigger trigger, HashMap<String, Object> original) {
        DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

        HashMap<String, Object> neu = new HashMap<> (original);
        neu.put(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name());
        neu.put(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval());
        neu.put(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
        neu.put(TRIGGER_START_TIME_OF_DAY, toNanoSinceMidnight(t.getStartTimeOfDay()));
        neu.put(TRIGGER_END_TIME_OF_DAY, toNanoSinceMidnight(t.getEndTimeOfDay()));
        
        return neu;
    }

    private long toNanoSinceMidnight(TimeOfDay tod) {
        
        long secondsSinceMidnight = (tod.getHour() * 3600) + (tod.getMinute() * 60 ) + tod.getSecond();
        
        return secondsSinceMidnight * 1000000;
    }

    /**
     * 
     * @param tod in seconds from midnight
     * @return 
     */
    private TimeOfDay fromCalendar(Calendar calendar) {
        
        return new TimeOfDay(calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Row stored) {

        DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

        String interval_unit = stored.getString(TRIGGER_REPEAT_INTERVAL_UNIT);
        if (interval_unit != null) {
            t.setRepeatIntervalUnit(DateBuilder.IntervalUnit.valueOf(interval_unit));
        }
        
        if (!stored.isNull(TRIGGER_REPEAT_INTERVAL)) {
            
            int repeatInterval = stored.getInt(TRIGGER_REPEAT_INTERVAL);

            t.setRepeatInterval(repeatInterval);
        }
        if (!stored.isNull(TRIGGER_TIMES_TRIGGERED)) {
            
            int timesTriggered = stored.getInt(TRIGGER_TIMES_TRIGGERED);

            t.setTimesTriggered(timesTriggered);
        }

        if (!stored.isNull(TRIGGER_START_TIME_OF_DAY)) {

            //getTime comes in nano seconds
            long startTOD = stored.getTime(TRIGGER_START_TIME_OF_DAY)/1000000;
            
            Date date = new Date(startTOD);
            Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
            calendar.setTime(date);
            
            t.setStartTimeOfDay(fromCalendar(calendar));
        }
        if (!stored.isNull(TRIGGER_END_TIME_OF_DAY)) {
            
            //getTime comes in nano seconds
            long endTOD = stored.getTime(TRIGGER_END_TIME_OF_DAY)/1000000;

            Date date = new Date(endTOD);
            Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
            calendar.setTime(date);
            
            t.setEndTimeOfDay(fromCalendar(calendar));
        }
    }
}
