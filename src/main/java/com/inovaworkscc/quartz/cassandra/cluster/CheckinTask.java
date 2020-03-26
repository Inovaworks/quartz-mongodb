package com.inovaworkscc.quartz.cassandra.cluster;

import com.inovaworkscc.quartz.cassandra.dao.SchedulerDao;
import com.inovaworkscc.quartz.cassandra.db.CassandraDatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The responsibility of this class is to check-in inside Scheduler Cluster.
 */
public class CheckinTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CheckinTask.class);

    private SchedulerDao schedulerDao;
    private Runnable errorhandler;

    public CheckinTask(SchedulerDao schedulerDao, Runnable errorHandler) {
        this.schedulerDao = schedulerDao;
        this.errorhandler = errorHandler;
    }

    // for tests only
    public void setErrorHandler(Runnable errorHandler) {
        this.errorhandler = errorHandler;
    }

    @Override
    public void run() {
        log.debug("Node {}:{} checks-in.", schedulerDao.schedulerName, schedulerDao.instanceId);
        try {
            schedulerDao.checkIn();
        } catch (CassandraDatabaseException e) {
            log.error("Node " + schedulerDao.instanceId + " could not check-in: " + e.getMessage(), e);
            errorhandler.run();
        }
    }
}
