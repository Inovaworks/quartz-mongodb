package com.inovaworkscc.quartz.cassandra.util;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.dao.SchedulerDao;
import com.inovaworkscc.quartz.cassandra.Constants;
import com.inovaworkscc.quartz.cassandra.cluster.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ExpiryCalculator {

    private static final Logger log = LoggerFactory.getLogger(ExpiryCalculator.class);

    private final SchedulerDao schedulerDao;
    private final Clock clock;
    private final long jobTimeoutMillis;
    private final long triggerTimeoutMillis;

    public ExpiryCalculator(SchedulerDao schedulerDao, Clock clock,
                            long jobTimeoutMillis, long triggerTimeoutMillis) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public boolean isJobLockExpired(Row lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isTriggerLockExpired(Row lock) {
        String schedulerId = lock.getString(Constants.LOCK_INSTANCE_ID);
        return isLockExpired(lock, triggerTimeoutMillis) && hasDefunctScheduler(schedulerId);
    }

    private boolean hasDefunctScheduler(String schedulerId) {
        Scheduler scheduler = schedulerDao.findInstance(schedulerId);
        if (scheduler == null) {
            log.debug("No such scheduler: {}", schedulerId);
            return false;
        }
        return scheduler.isDefunct(clock.millis()) && schedulerDao.isNotSelf(scheduler);
    }

    private boolean isLockExpired(Row lock, long timeoutMillis) {
        Date lockTime = lock.getTimestamp(Constants.LOCK_TIME);
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
