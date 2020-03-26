package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.db.CassandraDatabaseException;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static com.inovaworkscc.quartz.cassandra.Constants.LOCK_INSTANCE_ID;
import static com.inovaworkscc.quartz.cassandra.Constants.LOCK_TIME;
import com.inovaworkscc.quartz.cassandra.util.Clock;
import static com.inovaworkscc.quartz.cassandra.util.Keys.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class LocksDao implements GroupedDao{

    private static final Logger log = LoggerFactory.getLogger(LocksDao.class);

    public static final String TABLE_NAME_LOCKS = "locks";
    
    public static final String LOCKS_GET_ALL = CassandraConnectionManager.registerStatement ("JOBS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_LOCKS
    );
    
    public static final String LOCKS_GET_BY_KEY = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND "
                    + LOCK_TYPE + " = ?"
    );
    
    public static final String LOCKS_GET_BY_INSTANCE_ID = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_INSTANCE_ID",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + LOCK_INSTANCE_ID + " = ? AND "
                    + LOCK_TYPE + " = ? "
                    + "ALLOW FILTERING"
    );
    
    public static final String LOCKS_GET_BY_KEY_TYPE_INSTANCE_ID = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_TYPE_INSTANCE_ID",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND "
                    + LOCK_TYPE + " = ? AND"
                    + LOCK_INSTANCE_ID + " = ? "
                    + "ALLOW FILTERING"
    );
    
    public static final String LOCKS_GET_BY_KEY_INSTANCE_ID = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_INSTANCE_ID",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND "
                    + LOCK_INSTANCE_ID + " = ? "
                    + "ALLOW FILTERING"
    );
    
    public static final String LOCKS_GET_BY_KEY_TIME = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_TIME",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND "
                    + LOCK_TYPE + " = ? AND"
                    + LOCK_TIME + " = ?"
    );
    
    public static final String LOCKS_INSERT = CassandraConnectionManager.registerStatement("LOCKS_INSERT",
            "INSERT INTO " + TABLE_NAME_LOCKS + " (" + LOCK_TYPE + "," + KEY_GROUP + "," + KEY_NAME + "," + LOCK_INSTANCE_ID + "," + LOCK_TIME + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    public static final String LOCKS_DELETE = CassandraConnectionManager.registerStatement("LOCKS_DELETE",
        "DELETE FROM " + TABLE_NAME_LOCKS + " WHERE "
            + LOCK_TYPE + " = ? AND "
            + KEY_GROUP + " = ? AND "
            + KEY_NAME + " = ? AND "
            + LOCK_TIME + " = ? AND "
            + LOCK_INSTANCE_ID + " = ?"
    );
    
    public static final String LOCKS_DELETE_KEY = CassandraConnectionManager.registerStatement("LOCKS_DELETE_KEY",
        "DELETE FROM " + TABLE_NAME_LOCKS + " WHERE "
            + LOCK_TYPE + " = ? AND "
            + KEY_GROUP + " = ? AND "
            + KEY_NAME + " = ?"
    );
    
    public static final String LOCKS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("LOCKS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_LOCKS
    );
    
    public static final String LOCKS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    public static final String LOCKS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
     
    private Clock clock;
    public final String instanceId;

    public LocksDao(Clock clock, String instanceId) {
        this.clock = clock;
        this.instanceId = instanceId;
    }

    public List<Row> getCollection() {

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_ALL));
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();  
    }

    public Row findJobLock(JobKey job) {
               
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY));
        boundStatement.bind(job.getName(), job.getGroup(), LockType.j.name());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.one();  
    }

    public Row findTriggerLock(TriggerKey trigger) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY));
        boundStatement.bind(trigger.getName(), trigger.getGroup(), LockType.t.name());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.one();  
    }
    
    public Row findTriggerLockByTime(TriggerKey trigger, Date time) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_TIME));
        boundStatement.bind(trigger.getName(), trigger.getGroup(), LockType.t.name(), time.getTime());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.one();  
    }
    
    public List<Row> findTriggerLockAll(TriggerKey trigger) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY));
        boundStatement.bind(trigger.getName(), trigger.getGroup(), LockType.t.name());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();  
    }

    public List<TriggerKey> findOwnTriggersLocks() {
        
        final List<TriggerKey> ret = new LinkedList<>();

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_INSTANCE_ID));
        boundStatement.bind(instanceId, LockType.t.name());
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        rs.getUninterruptibly().forEach(row -> {
            ret.add(toTriggerKey(row));
        });
        
        return ret;
    }

    public void lockJob(JobDetail job) {
        log.debug("Inserting lock for job {}", job.getKey());
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_INSERT));
        boundStatement.bind(LockType.j.name(), job.getKey().getGroup(), job.getKey().getName(), instanceId, clock.now().getTime());
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void lockTrigger(TriggerKey key) {
        log.info("Inserting lock for trigger {}", key);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_INSERT));
        boundStatement.bind(LockType.t.name(), key.getGroup(), key.getName(), instanceId, clock.now().getTime());
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }
    
    /**
     * Lock given trigger iff its <b>lockTime</b> haven't changed.
     *
     * <p>Update is performed using "Update row if current" pattern
     * to update iff row in DB hasn't changed - haven't been relocked
     * by other scheduler.</p>
     *
     * @param key         identifies trigger lock
     * @param lockTime    expected current lockTime
     * @return false when not found or caught an exception
     */
    public boolean relock(TriggerKey key, Date lockTime) {
        
        boolean ret = false;

        try{
            
            Row trigerLock = findTriggerLockByTime(key, lockTime);
        
            if(trigerLock != null){

                lockTrigger(key);

                remove(trigerLock);

                ret = true;
                
            }else{
                ret = false;
            }
            
            log.info("Scheduler {} couldn't relock the trigger {} with lock time: {}",
                instanceId, key, lockTime.getTime());
                    
            return ret;
        } catch (CassandraDatabaseException e){
            log.error("Relock failed because: " + e.getMessage(), e);
            return false;
        }
    }

    /**
     * Reset lock time on own lock.
     *
     * @throws JobPersistenceException in case of errors from Cassandra
     * @param key    trigger whose lock to refresh
     * @return true on successful update
     */
    public boolean updateOwnLock(TriggerKey key) throws JobPersistenceException {

        List<Boolean> wasApplied = new ArrayList<>();
        
        try{
            
            BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_TYPE_INSTANCE_ID));
            boundStatement.bind(key.getName(), key.getGroup(), LockType.t.name(), instanceId) ;
            List<Row> rsList = CassandraConnectionManager.getInstance().execute(boundStatement).all(); 

            wasApplied.add(!rsList.isEmpty());
            
            lockTrigger(key);
            
            rsList.forEach((row) -> {
                remove(row);
            });
                     
        } catch (CassandraDatabaseException e){
            log.error("Relock failed because: " + e.getMessage(), e);
            return false;
        }
        
        return wasApplied.contains(Boolean.TRUE);
    }

    public void remove(Row lock) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_DELETE));
        boundStatement.bind(lock.getString(LOCK_TYPE), lock.getString(KEY_GROUP), lock.getString(KEY_NAME), lock.getString(LOCK_TIME), lock.getString(LOCK_INSTANCE_ID));
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    /**
     * Unlock the trigger if it still belongs to the current scheduler.
     *
     * @param trigger    to unlock
     */
    public void unlockTrigger(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_INSTANCE_ID));
        boundStatement.bind(trigger.getKey().getName(), trigger.getKey().getGroup(), instanceId) ;
        List<Row> rsList = CassandraConnectionManager.getInstance().execute(boundStatement).all(); 

        rsList.forEach((row) -> {        
            remove(row);
        });
        
        log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }

    public void unlockJob(JobDetail job) {
        log.debug("Removing lock for job {}", job.getKey());
        remove(job.getKey());
    }

    private void remove(JobKey jobKey) {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_DELETE_KEY));
        boundStatement.bind(LockType.t, jobKey.getGroup(), jobKey.getName());
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }
    
    @Override
    public Set<String> groupsLike(String value) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_GROUP_LIKE));
        boundStatement.bind(value);
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        Set<String> groups = new HashSet<>();
        rs.getUninterruptibly().forEach(row -> {
            groups.add(row.getString(KEY_GROUP));
        });
                
        return groups;
    }

    @Override
    public List<Row> rowsInGroups(Set<String> groups) {

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_GROUP));
        boundStatement.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        Set<String> ret = new HashSet<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_DISTINCT_KEY_GROUP));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret; 
    }
}
