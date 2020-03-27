package com.inovaworkscc.quartz.cassandra;

import com.inovaworkscc.quartz.cassandra.dao.PausedJobGroupsDao;
import com.inovaworkscc.quartz.cassandra.dao.CalendarDao;
import com.inovaworkscc.quartz.cassandra.dao.TriggerDao;
import com.inovaworkscc.quartz.cassandra.dao.SchedulerDao;
import com.inovaworkscc.quartz.cassandra.dao.LocksDao;
import com.inovaworkscc.quartz.cassandra.dao.JobDao;
import com.inovaworkscc.quartz.cassandra.cluster.TriggerRecoverer;
import com.inovaworkscc.quartz.cassandra.cluster.CheckinExecutor;
import com.inovaworkscc.quartz.cassandra.cluster.RecoveryTriggerFactory;
import com.inovaworkscc.quartz.cassandra.cluster.CheckinTask;
import com.inovaworkscc.quartz.cassandra.cluster.KamikazeErrorHandler;
import com.inovaworkscc.quartz.cassandra.dao.PausedTriggerGroupsDao;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.trigger.MisfireHandler;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerConverter;
import com.inovaworkscc.quartz.cassandra.util.ExpiryCalculator;
import com.inovaworkscc.quartz.cassandra.util.Clock;
import com.inovaworkscc.quartz.cassandra.util.QueryHelper;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import java.util.Properties;

public class CassandraStoreAssembler {

    public JobCompleteHandler jobCompleteHandler;
    public LockManager lockManager;
    public TriggerStateManager triggerStateManager;
    public TriggerRunner triggerRunner;
    public TriggerAndJobPersister persister;

    public CalendarDao calendarDao;
    public JobDao jobDao;
    public LocksDao locksDao;
    public SchedulerDao schedulerDao;
    public PausedJobGroupsDao pausedJobGroupsDao;
    public PausedTriggerGroupsDao pausedTriggerGroupsDao;
    public TriggerDao triggerDao;

    public TriggerRecoverer triggerRecoverer;
    public CheckinExecutor checkinExecutor;

    private QueryHelper queryHelper = new QueryHelper();
    private TriggerConverter triggerConverter;

    public void build(CassandraJobStore jobStore, ClassLoadHelper loadHelper,
                      SchedulerSignaler signaler, Properties quartzProps)
        throws SchedulerConfigException, ClassNotFoundException,
        IllegalAccessException, InstantiationException {
        
        CassandraConnectionManager.setProperties(
                jobStore.getContactPoint(), 
                jobStore.getPort(), 
                jobStore.getDbName());
        
        JobDataConverter jobDataConverter = new JobDataConverter(jobStore.isJobDataAsBase64());

        jobDao = createJobDao(jobStore, loadHelper, jobDataConverter);

        triggerConverter = new TriggerConverter(jobDao, jobDataConverter);

        triggerDao = createTriggerDao(jobStore);
        calendarDao = createCalendarDao(jobStore);
        locksDao = createLocksDao(jobStore);
        pausedJobGroupsDao = createPausedJobGroupsDao(jobStore);
        pausedTriggerGroupsDao = createPausedTriggerGroupsDao(jobStore);
        schedulerDao = createSchedulerDao(jobStore);

        persister = createTriggerAndJobPersister();

        jobCompleteHandler = createJobCompleteHandler(signaler);

        lockManager = createLockManager(jobStore);

        triggerStateManager = createTriggerStateManager();

        MisfireHandler misfireHandler = createMisfireHandler(jobStore, signaler);

        RecoveryTriggerFactory recoveryTriggerFactory
                = new RecoveryTriggerFactory(jobStore.instanceId);

        triggerRecoverer = new TriggerRecoverer(locksDao, persister,
                lockManager, triggerDao, jobDao, recoveryTriggerFactory,
                misfireHandler);

        triggerRunner = createTriggerRunner(misfireHandler);

        checkinExecutor = createCheckinExecutor(jobStore, loadHelper, quartzProps);
    }

    private CheckinExecutor createCheckinExecutor(CassandraJobStore jobStore, ClassLoadHelper loadHelper,
                                                  Properties quartzProps)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return new CheckinExecutor(createCheckinTask(jobStore, loadHelper),
                jobStore.clusterCheckinIntervalMillis, jobStore.instanceId);
    }

    private Runnable createCheckinTask(CassandraJobStore jobStore, ClassLoadHelper loadHelper)
        throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Runnable errorHandler;
        Class aClass;
        if (jobStore.getCheckInErrorHandler() == null) {
            // current default, see 
            aClass = KamikazeErrorHandler.class;
        } else {
            aClass = loadHelper.loadClass(jobStore.getCheckInErrorHandler());
        }
        errorHandler = (Runnable) aClass.newInstance();
        return new CheckinTask(schedulerDao, errorHandler);
    }

    private CalendarDao createCalendarDao(CassandraJobStore jobStore) {
        return new CalendarDao();
    }

    private JobDao createJobDao(CassandraJobStore jobStore, ClassLoadHelper loadHelper, JobDataConverter jobDataConverter) {
        JobConverter jobConverter = new JobConverter(jobStore.getClassLoaderHelper(loadHelper), jobDataConverter);
        return new JobDao(queryHelper, jobConverter);
    }

    private JobCompleteHandler createJobCompleteHandler(SchedulerSignaler signaler) {
        return new JobCompleteHandler(persister, signaler, jobDao, locksDao, triggerDao);
    }

    private LocksDao createLocksDao(CassandraJobStore jobStore) {
        return new LocksDao(Clock.SYSTEM_CLOCK, jobStore.instanceId);
    }

    private LockManager createLockManager(CassandraJobStore jobStore) {
        ExpiryCalculator expiryCalculator = new ExpiryCalculator(schedulerDao,
                Clock.SYSTEM_CLOCK, jobStore.jobTimeoutMillis, jobStore.triggerTimeoutMillis);
        return new LockManager(locksDao, expiryCalculator);
    }

    private MisfireHandler createMisfireHandler(CassandraJobStore jobStore, SchedulerSignaler signaler) {
        return new MisfireHandler(calendarDao, signaler, jobStore.misfireThreshold);
    }

    private PausedJobGroupsDao createPausedJobGroupsDao(CassandraJobStore jobStore) {
        return new PausedJobGroupsDao();
    }

    private PausedTriggerGroupsDao createPausedTriggerGroupsDao(CassandraJobStore jobStore) {
        return new PausedTriggerGroupsDao();
    }

    private SchedulerDao createSchedulerDao(CassandraJobStore jobStore) {
        return new SchedulerDao(jobStore.schedulerName, jobStore.instanceId, jobStore.clusterCheckinIntervalMillis,
                Clock.SYSTEM_CLOCK);
    }

    private TriggerAndJobPersister createTriggerAndJobPersister() {
        return new TriggerAndJobPersister(triggerDao, jobDao, triggerConverter);
    }

    private TriggerDao createTriggerDao(CassandraJobStore jobStore) {
        return new TriggerDao(queryHelper, triggerConverter);
    }

    private TriggerRunner createTriggerRunner(MisfireHandler misfireHandler) {
        return new TriggerRunner(persister, triggerDao, jobDao, locksDao, calendarDao,
                misfireHandler, triggerConverter, lockManager, triggerRecoverer);
    }

    private TriggerStateManager createTriggerStateManager() {
        return new TriggerStateManager(triggerDao, jobDao,
                pausedJobGroupsDao, pausedTriggerGroupsDao, queryHelper);
    }
}
