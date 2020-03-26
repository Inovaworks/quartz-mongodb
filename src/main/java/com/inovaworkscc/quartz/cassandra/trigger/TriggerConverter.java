package com.inovaworkscc.quartz.cassandra.trigger;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.JobDataConverter;
import com.inovaworkscc.quartz.cassandra.Constants;
import com.inovaworkscc.quartz.cassandra.dao.JobDao;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_NAME;
import java.util.HashMap;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerConverter {

    public static final String TRIGGER_CALENDAR_NAME = "calendarName";
    public static final String TRIGGER_CLASS = "class";
    public static final String TRIGGER_DESCRIPTION = "description";
    public static final String TRIGGER_END_TIME = "endTime";
    public static final String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
    public static final String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
    public static final String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
    public static final String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
    public static final String TRIGGER_PRIORITY = "priority";
    public static final String TRIGGER_START_TIME = "startTime";

    private static final Logger log = LoggerFactory.getLogger(TriggerConverter.class);

    private JobDao jobDao;
    private final JobDataConverter jobDataConverter;

    public TriggerConverter(JobDao jobDao, JobDataConverter jobDataConverter) {
        this.jobDao = jobDao;
        this.jobDataConverter = jobDataConverter;
    }

    /**
     * Converts trigger into HashMap.
     * Depending on the config, job data map can be stored
     * as a {@code base64} encoded (default) or plain object.
     * @param newTrigger
     * @param jobId
     * @return 
     * @throws org.quartz.JobPersistenceException
     */
    public HashMap<String, Object> toHashMap(OperableTrigger newTrigger, String jobId)
            throws JobPersistenceException {
        
        HashMap<String, Object> trigger = convertToHashMap(newTrigger, jobId);
        jobDataConverter.toHashMap(newTrigger.getJobDataMap(), trigger);

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(newTrigger);
        trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);
        return trigger;
    }

    /**
     * Restore trigger from Cassandra Row.
     *
     * @param triggerKey {@link TriggerKey} instance.
     * @param triggerRow cassandra {@link Row} to read from.
     * @return trigger from Row or null when trigger has no associated job
     * @throws JobPersistenceException if could not construct trigger instance
     * or could not deserialize job data map.
     */
    public OperableTrigger toTrigger(TriggerKey triggerKey, Row triggerRow)
            throws JobPersistenceException {
        OperableTrigger trigger = toTriggerWithOptionalJob(triggerKey, triggerRow);
        if ( trigger.getJobKey() == null) {
            return null;
        }
        return trigger;
    }

    /**
     * Restore trigger from Cassandra Row.
     *
     * @param triggerKey {@link TriggerKey} instance.
     * @param triggerRow cassandra {@link Row} to read from.
     * @return trigger from Row even if no associated job exists
     * @throws JobPersistenceException if could not construct trigger instance
     * or could not deserialize job data map.
     */
    public OperableTrigger toTriggerWithOptionalJob(TriggerKey triggerKey, Row triggerRow) throws JobPersistenceException {
        OperableTrigger trigger = createNewInstance(triggerRow);

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(trigger);

        loadCommonProperties(triggerKey, triggerRow, trigger);

        jobDataConverter.toJobData(triggerRow, trigger.getJobDataMap());

        loadStartAndEndTimes(triggerRow, trigger);

        tpd.setExtraPropertiesAfterInstantiation(trigger, triggerRow);

        String jobId = triggerRow.getString(Constants.TRIGGER_JOB_ID);
        Row job = jobDao.getById(jobId);

        if (job != null) {
            trigger.setJobKey(new JobKey(job.getString(KEY_NAME), job.getString(KEY_GROUP)));
        }
        return trigger;
    }

    public OperableTrigger toTrigger(Row row) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(row.getString(KEY_NAME), row.getString(KEY_GROUP));
        return toTrigger(key, row);
    }

    public OperableTrigger toTriggerWithOptionalJob(Row row) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(row.getString(KEY_NAME), row.getString(KEY_GROUP));
        return toTriggerWithOptionalJob(key, row);
    }

    private HashMap<String, Object> convertToHashMap(OperableTrigger newTrigger, String jobId) {
        HashMap<String, Object> trigger = new HashMap();
        
        trigger.put(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
        trigger.put(TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
        trigger.put(TRIGGER_CLASS, newTrigger.getClass().getName());
        trigger.put(TRIGGER_DESCRIPTION, newTrigger.getDescription());
        trigger.put(TRIGGER_END_TIME, newTrigger.getEndTime());
        trigger.put(TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
        trigger.put(TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
        trigger.put(Constants.TRIGGER_JOB_ID, jobId);
        trigger.put(KEY_NAME, newTrigger.getKey().getName());
        trigger.put(KEY_GROUP, newTrigger.getKey().getGroup());
        trigger.put(KEY_GROUP + "_index", newTrigger.getKey().getGroup());
        trigger.put(TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
        trigger.put(Constants.TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
        trigger.put(TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
        trigger.put(TRIGGER_PRIORITY, newTrigger.getPriority());
        trigger.put(TRIGGER_START_TIME, newTrigger.getStartTime());
        
        return trigger;
    }

    private OperableTrigger createNewInstance(Row triggerRow) throws JobPersistenceException {
        String triggerClassName = triggerRow.getString(TRIGGER_CLASS);
        try {
            @SuppressWarnings("unchecked")
            Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader()
                    .loadClass(triggerClassName);
            return triggerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not find trigger class " + triggerClassName);
        } catch (Exception e) {
            throw new JobPersistenceException("Could not instantiate trigger class " + triggerClassName);
        }
    }

    private ClassLoader getTriggerClassLoader() {
        return Job.class.getClassLoader();
    }

    private void loadCommonProperties(TriggerKey triggerKey, Row triggerRow, OperableTrigger trigger) {
        trigger.setKey(triggerKey);
        trigger.setCalendarName(triggerRow.getString(TRIGGER_CALENDAR_NAME));
        trigger.setDescription(triggerRow.getString(TRIGGER_DESCRIPTION));
        trigger.setFireInstanceId(triggerRow.getString(TRIGGER_FIRE_INSTANCE_ID));
        trigger.setMisfireInstruction(triggerRow.getInt(TRIGGER_MISFIRE_INSTRUCTION));
        trigger.setNextFireTime(triggerRow.getTimestamp(Constants.TRIGGER_NEXT_FIRE_TIME));
        trigger.setPreviousFireTime(triggerRow.getTimestamp(TRIGGER_PREVIOUS_FIRE_TIME));
        trigger.setPriority(triggerRow.getInt(TRIGGER_PRIORITY));
    }

    private void loadStartAndEndTimes(Row triggerRow, OperableTrigger trigger) {
        loadStartAndEndTime(triggerRow, trigger);
    }

    private void loadStartAndEndTime(Row triggerRow, OperableTrigger trigger) {
        try {
            trigger.setStartTime(triggerRow.getTimestamp(TRIGGER_START_TIME));
            trigger.setEndTime(triggerRow.getTimestamp(TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }
    }
}
