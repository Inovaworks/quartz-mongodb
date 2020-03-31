package com.inovaworkscc.quartz.cassandra;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.dao.JobDao;
import com.inovaworkscc.quartz.cassandra.dao.TriggerDao;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerConverter;
import com.inovaworkscc.quartz.cassandra.util.Keys;

import java.util.HashMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TriggerAndJobPersister {

    private static final Logger LOG = LoggerFactory.getLogger(TriggerAndJobPersister.class);

    private final TriggerDao triggerDao;
    private final JobDao jobDao;
    private TriggerConverter triggerConverter;

    public TriggerAndJobPersister(TriggerDao triggerDao, JobDao jobDao, TriggerConverter triggerConverter) {
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.triggerConverter = triggerConverter;
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        final Row row = jobDao.getJob(jobKey);
        return triggerDao.getTriggersForJob(row);
    }

    public boolean removeJob(JobKey jobKey) {
        Row item = jobDao.getJob(jobKey);
        if (item != null) {
            jobDao.remove(jobKey);
            triggerDao.removeByJobId(item.getString(Constants.TRIGGER_JOB_ID));
            return true;
        }
        return false;
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        jobKeys.forEach((key) -> {
            removeJob(key);
        });
        return false;
    }

    public boolean removeTrigger(TriggerKey triggerKey) {
        Row trigger = triggerDao.findTrigger(triggerKey);
        if (trigger != null) {
            removeOrphanedJob(trigger);
            //TODO: check if can .deleteOne(filter) here
            triggerDao.remove(triggerKey);
            return true;
        }
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        //FIXME return boolean allFound = true when all removed
        for (TriggerKey key : triggerKeys) {
            removeTrigger(key);
        }
        return false;
    }

    public boolean removeTriggerWithoutNextFireTime(OperableTrigger trigger) {
        if (trigger.getNextFireTime() == null) {
            LOG.info("Removing trigger {} as it has no next fire time.", trigger.getKey());
            removeTrigger(trigger.getKey());
            return true;
        }
        return false;
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
            throws JobPersistenceException {
        OperableTrigger oldTrigger = triggerDao.getTrigger(triggerKey);
        if (oldTrigger == null) {
            return false;
        }

        if (!oldTrigger.getJobKey().equals(newTrigger.getJobKey())) {
            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
        }

        removeOldTrigger(triggerKey);
        copyOldJobDataMap(newTrigger, oldTrigger);
        storeTrigger(newTrigger, true);

        return true;
    }

    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
            throws JobPersistenceException {
        String jobId = jobDao.storeJobInCassandra(newJob, false);

        LOG.debug("Storing job {} and trigger {}", newJob.getKey(), newTrigger.getKey());
        storeTrigger(newTrigger, jobId, false);
    }

    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws JobPersistenceException {
        if (newTrigger.getJobKey() == null) {
            throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
        }
           
        Row row = jobDao.getJob(newTrigger.getJobKey());
        if (row != null) {
            storeTrigger(newTrigger, row.getString(Constants.TRIGGER_JOB_ID), replaceExisting);
        } else {
            throw new JobPersistenceException("Could not find job with key " + newTrigger.getJobKey());
        }
    }

    private void copyOldJobDataMap(OperableTrigger newTrigger, OperableTrigger trigger) {
        // Copy across the job data map from the old trigger to the new one.
        newTrigger.getJobDataMap().putAll(trigger.getJobDataMap());
    }

    private boolean isNotDurable(Row job) {
        
        boolean isDurabilitySet = !job.isNull(JobConverter.JOB_DURABILITY);
        boolean durability = job.getBool(JobConverter.JOB_DURABILITY);
        
        return !isDurabilitySet ||
                durability;
    }

    private boolean isOrphan(Row job) {
        return (job != null) && isNotDurable(job) && triggerDao.hasLastTrigger(job);
    }

    private void removeOldTrigger(TriggerKey triggerKey) {
        // Can't call remove trigger as if the job is not durable, it will remove the job too
        triggerDao.remove(triggerKey);
    }

    // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
    // then the job should be removed also.
    private void removeOrphanedJob(Row trigger) {
        if (!trigger.isNull(Constants.TRIGGER_JOB_ID)) {
            // There is only 1 job per trigger so no need to look further.
            Row job = jobDao.getById(trigger.getString(Constants.TRIGGER_JOB_ID));
            if (isOrphan(job)) {
                jobDao.remove(new JobKey(job.getString(Keys.KEY_NAME), job.getString(Keys.KEY_GROUP)));
            }
        } else {
            LOG.debug("The trigger had no associated jobs");
        }
    }

    private void storeNewTrigger(OperableTrigger newTrigger, OperableTrigger oldTrigger)
            throws JobPersistenceException {
        try {
            storeTrigger(newTrigger, false);
        } catch (JobPersistenceException jpe) {
            storeTrigger(oldTrigger, false);
            throw jpe;
        }
    }

    private void storeTrigger(OperableTrigger newTrigger, String jobId, boolean replaceExisting)
            throws JobPersistenceException {
        HashMap<String, Object> trigger = triggerConverter.toHashMap(newTrigger, jobId);
        if (replaceExisting) {
            triggerDao.replace(newTrigger.getKey(), trigger);
        } else {
            triggerDao.insert(trigger, newTrigger);
        }
    }
}
