package com.inovaworkscc.quartz.cassandra;

import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import org.quartz.*;
import org.quartz.Calendar;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

public class CassandraJobStore implements JobStore {

    private CassandraStoreAssembler assembler = new CassandraStoreAssembler();

    String authDbName;
    String schedulerName;
    String instanceId;
    long misfireThreshold = 5000;
    long triggerTimeoutMillis = 10 * 60 * 1000L;
    long jobTimeoutMillis = 10 * 60 * 1000L;
    private boolean clustered = false;
    long clusterCheckinIntervalMillis = 7500;
    boolean jobDataAsBase64 = true;
    String checkInErrorHandler = null;

    public static final String PROPERTIES_FILE_NAME = "quartz.properties";

    public CassandraJobStore() {
    }
    
    /**
     * Override to change class loading mechanism, to e.g. dynamic
     *
     * @param original default provided by Quartz
     * @return loader to use for loading of Quartz Jobs' classes
     */
    protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
        return original;
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
            throws SchedulerConfigException {
        Properties props = loadProperties(loadHelper);
        try {
            assembler.build(this, loadHelper, signaler, props);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new SchedulerConfigException("Failed to instantiate cluster checkin error handler", e);
        }

        if (isClustered()) {
            try {
                assembler.triggerRecoverer.recover();
            } catch (JobPersistenceException e) {
                throw new SchedulerConfigException("Cannot recover triggers", e);
            }
            assembler.checkinExecutor.start();
        }
    }

    private Properties loadProperties(ClassLoadHelper loadHelper) {
        Properties props = new Properties();
        File propFile = new File(PROPERTIES_FILE_NAME);
        InputStream is = null;
        // this roughly mimics how StdSchedulerFactory#initialize() fills properties in
        try {
            if (propFile.exists()) {
                is = new BufferedInputStream(new FileInputStream(PROPERTIES_FILE_NAME));
                props.load(is);
            } else {
                is = loadHelper.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME);
                if (is != null) {
                    props.load(is);
                }
            }
        } catch (IOException e) {
            // ignore
        }
        return props;
    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        // No-op
    }

    @Override
    public void schedulerPaused() {
        // No-op
    }

    @Override
    public void schedulerResumed() {
    }

    @Override
    public void shutdown() {
        assembler.checkinExecutor.shutdown();
        CassandraConnectionManager.getInstance().shutdownConnection();
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        // this will vary...
        return 200;
    }

    @Override
    public long getAcquireRetryDelay(int failureCount) {
        return CassandraConnectionManager.getInstance().getSession().getCluster().getConfiguration().getSocketOptions().getConnectTimeoutMillis();
    }

    /**
     * Set whether this instance is part of a cluster.
     */
    public void setIsClustered(boolean isClustered) {
        this.clustered = isClustered;
    }

    @Override
    public boolean isClustered() {
        return clustered;
    }

    /**
     * Set the frequency (in milliseconds) at which this instance "checks-in"
     * with the other instances of the cluster.
     * <p>
     * Affects the rate of detecting failed instances.
     */
    public void setClusterCheckinInterval(long clusterCheckinInterval) {
        this.clusterCheckinIntervalMillis = clusterCheckinInterval;
    }

    public boolean isJobDataAsBase64() {

        return jobDataAsBase64;
    }

    /**
     * Configures the way job data is stored. {@link JobDetail}'s
     * or {@link Trigger}'s {@link JobDataMap} can be represented
     * as a {@code Map<String,Object>}.
     * <ul>
     * <li><b>{@code true}</b> (default) - Serialize map with
     * {@link java.io.ObjectOutputStream ObjectOutputStream}
     * and store as {@code base64} encoded string in field
     * '{@value Constants#JOB_DATA}'. Map may contain any
     * {@link java.io.Serializable Serializable} object
     * internally, but will have some performance impact.</li>
     * <li><b>{@code false}</b> - Store map directly in
     * '{@value Constants#JOB_DATA_PLAIN}' field. Use this
     * option is you only store simple types in job data
     * map for better performance.</li>
     * </ul>
     */
    public void setJobDataAsBase64(boolean jobDataAsBase64) {

        this.jobDataAsBase64 = jobDataAsBase64;
    }

    public String getCheckInErrorHandler() {
        return checkInErrorHandler;
    }

    public void setCheckInErrorHandler(String checkInErrorHandler) {
        this.checkInErrorHandler = checkInErrorHandler;
    }

    /**
     * Job and Trigger storage Methods
     */
    @Override
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
            throws JobPersistenceException {
        assembler.persister.storeJobAndTrigger(newJob, newTrigger);
    }

    @Override
    public void storeJob(JobDetail newJob, boolean replaceExisting)
            throws JobPersistenceException {
        assembler.jobDao.storeJobInCassandra(newJob, replaceExisting);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {

            JobDetail newJob = entry.getKey();
            Set<? extends Trigger> triggers = entry.getValue();
            assembler.jobDao.storeJobInCassandra(newJob, replace);

            // Store all triggers of the job.
            for (Trigger newTrigger : triggers) {
                // Simply cast to OperableTrigger as in QuartzScheduler.scheduleJobs
                // http://www.programcreek.com/java-api-examples/index.php?api=org.quartz.spi.OperableTrigger
                assembler.persister.storeTrigger((OperableTrigger) newTrigger, replace);
            }
        }
    }

    @Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        return assembler.persister.removeJob(jobKey);
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        return assembler.persister.removeJobs(jobKeys);
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return assembler.jobDao.retrieveJob(jobKey);
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws JobPersistenceException {
        assembler.persister.storeTrigger(newTrigger, replaceExisting);
    }

    @Override
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return assembler.persister.removeTrigger(triggerKey);
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return assembler.persister.removeTriggers(triggerKeys);
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        return assembler.persister.replaceTrigger(triggerKey, newTrigger);
    }

    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return assembler.triggerDao.getTrigger(triggerKey);
    }

    @Override
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return assembler.jobDao.exists(jobKey);
    }

    @Override
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return assembler.triggerDao.exists(triggerKey);
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        assembler.jobDao.clear();
        assembler.triggerDao.clear();
        assembler.calendarDao.clear();
        assembler.pausedJobGroupsDao.remove();
        assembler.pausedTriggerGroupsDao.remove();
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws JobPersistenceException {
        // TODO implement updating triggers
        if (updateTriggers) {
            throw new UnsupportedOperationException("Updating triggers is not supported.");
        }

        assembler.calendarDao.store(name, calendar);
    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        return assembler.calendarDao.remove(calName);
    }

    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        return assembler.calendarDao.retrieveCalendar(calName);
    }

    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return (int)assembler.jobDao.getCount();
    }

    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return (int)assembler.triggerDao.getCount();
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return (int)assembler.calendarDao.getCount();
    }

    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return assembler.jobDao.getJobKeys(matcher);
    }

    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return assembler.triggerDao.getTriggerKeys(matcher);
    }

    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return assembler.jobDao.getGroupNames();
    }

    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return assembler.triggerDao.getGroupNames();
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        return assembler.calendarDao.retrieveCalendarNames();
    }

    @Override
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        return assembler.persister.getTriggersForJob(jobKey);
    }

    @Override
    public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return assembler.triggerStateManager.getState(triggerKey);
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        assembler.triggerStateManager.pause(triggerKey);
    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return assembler.triggerStateManager.pause(matcher);
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        assembler.triggerStateManager.resume(triggerKey);
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return assembler.triggerStateManager.resume(matcher);
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return assembler.triggerStateManager.getPausedTriggerGroups();
    }

    // only for tests
    public Set<String> getPausedJobGroups() throws JobPersistenceException {
        return assembler.pausedJobGroupsDao.getPausedGroups();
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        assembler.triggerStateManager.pauseAll();
    }

    @Override
    public void resumeAll() throws JobPersistenceException {
        assembler.triggerStateManager.resumeAll();
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        assembler.triggerStateManager.pauseJob(jobKey);
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return assembler.triggerStateManager.pauseJobs(groupMatcher);
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        assembler.triggerStateManager.resume(jobKey);
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return assembler.triggerStateManager.resumeJobs(groupMatcher);
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        return assembler.triggerRunner.acquireNext(noLaterThan, maxCount, timeWindow);
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        assembler.lockManager.unlockAcquiredTrigger(trigger);
    }

    @Override
    public void resetTriggerFromErrorState(TriggerKey triggerKey) {
        assembler.triggerStateManager.resetTriggerFromErrorState(triggerKey);
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
            throws JobPersistenceException {
        return assembler.triggerRunner.triggersFired(triggers);
    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail job,
                                     CompletedExecutionInstruction triggerInstCode) {
        assembler.jobCompleteHandler.jobComplete(trigger, job, triggerInstCode);
    }

    @Override
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    public void setInstanceName(String schedName) {
        // Used as part of cluster node identifier:
        schedulerName = schedName;
    }

    @Override
    public void setThreadPoolSize(int poolSize) {
        // No-op
    }

    /*public List<Row> getJobCollection() {
        return assembler.jobDao.getCollection();
    }

    public List<Row> getTriggerCollection() {
        return assembler.triggerDao.getCollection();
    }

    public List<Row> getCalendarCollection() {
        return assembler.calendarDao.getCollection();
    }

    public List<Row> getLocksCollection() {
        return assembler.locksDao.getCollection();
    }*/

    public void setMisfireThreshold(long misfireThreshold) {
        this.misfireThreshold = misfireThreshold;
    }

    public void setTriggerTimeoutMillis(long triggerTimeoutMillis) {
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public void setJobTimeoutMillis(long jobTimeoutMillis) {
        this.jobTimeoutMillis = jobTimeoutMillis;
    }

    public String getAuthDbName() {
        return authDbName;
    }

    public void setAuthDbName(String authDbName) {
        this.authDbName = authDbName;
    }
}
