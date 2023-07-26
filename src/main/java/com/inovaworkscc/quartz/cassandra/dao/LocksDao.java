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
import java.util.HashSet;
import java.util.Set;
import org.quartz.utils.Key;

public class LocksDao implements GroupedDao{

    private static final String ALLOW_FILTERING = " ALLOW FILTERING";
    private static final Logger LOG = LoggerFactory.getLogger(LocksDao.class);

    public static final String TABLE_NAME_LOCKS = "locks";
    
    public static final String LOCKS_GET_ALL = CassandraConnectionManager.registerStatement ("LOCKS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_LOCKS + ALLOW_FILTERING
    );
    
    public static final String LOCKS_GET_BY_KEY_LOCK_TYPE = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_LOCK_TYPE",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ? AND"
                    + LOCK_TYPE + " = ?" + ALLOW_FILTERING
    );
    
    public static final String LOCKS_GET_BY_KEY = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ?" + ALLOW_FILTERING
    );
    
    public static final String LOCKS_GET_BY_INSTANCE_ID = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_INSTANCE_ID",
            "SELECT " + KEY_NAME + "," + KEY_GROUP + "," + LOCK_TYPE + " FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + LOCK_INSTANCE_ID + " = ? " + ALLOW_FILTERING
    );
    
    public static final String LOCKS_INSERT = CassandraConnectionManager.registerStatement("LOCKS_INSERT",
            "INSERT INTO " + TABLE_NAME_LOCKS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + LOCK_TYPE + "," + LOCK_INSTANCE_ID + "," + LOCK_TIME + ")" + " VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    public static final String LOCKS_UPDATE = CassandraConnectionManager.registerStatement("LOCKS_UPDATE",
            "UPDATE " + TABLE_NAME_LOCKS + " SET " 
                + LOCK_INSTANCE_ID + " = ? ,"
                + LOCK_TIME + " = ? "
                + "WHERE "
                + KEY_NAME + " = ? AND "
                + KEY_GROUP + " = ? AND "
                + LOCK_TYPE + " = ? "
                + "IF EXISTS" + ALLOW_FILTERING
    );
    
    public static final String LOCKS_DELETE = CassandraConnectionManager.registerStatement("LOCKS_DELETE",
        "DELETE FROM " + TABLE_NAME_LOCKS + " WHERE "
            + KEY_NAME + " = ? AND "
            + KEY_GROUP + " = ? AND "
            + LOCK_TYPE + " = ? "
    );
    
    public static final String LOCKS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("LOCKS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_LOCKS + ALLOW_FILTERING
    );
    
    public static final String LOCKS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_GROUP + " IN ?" + ALLOW_FILTERING
    );
    
    public static final String LOCKS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("LOCKS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_LOCKS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?" + ALLOW_FILTERING
    );
     
    private final Clock clock;
    public final String instanceId;

    public LocksDao(Clock clock, String instanceId) {
        this.clock = clock;
        this.instanceId = instanceId;
    }
    
    /**
     * remove all locks for this instance on startup
     * @param clustered 
     */
    public void prepareInstance(boolean clustered) {

        //TODO check what to do when is clustered
        
        ResultSetFuture rs = findOwnLocks(); 

        rs.getUninterruptibly().forEach(row -> {
            
            if(LockType.t.name().equals(row.getString(LOCK_TYPE))){
                remove(row);
            }
        });  
    }
    
    public Row findJobLock(JobKey job) {
               
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_LOCK_TYPE));
        boundStatement.bind(job.getName(), job.getGroup(), LockType.j.name());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);
        
        return rs.one();  
    }

    public Row findTriggerLock(TriggerKey trigger) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_LOCK_TYPE));
        boundStatement.bind(trigger.getName(), trigger.getGroup(), LockType.t.name());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);
        
        return rs.one();   
    }


    public List<Row> findAllLocks(Key key) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY));
        boundStatement.bind(key.getName(), key.getGroup());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);
        
        return rs.all();   
    }

    public Row findTriggerLockByTime(TriggerKey trigger, Date time) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_KEY_LOCK_TYPE));
        boundStatement.bind(trigger.getName(), trigger.getGroup(), LockType.t.name());
        List<Row> rs = CassandraConnectionManager.getInstance().execute(boundStatement).all();
        
        for (Row row : rs) {
            if (time.equals(row.getTimestamp(LOCK_TIME))) {
                return row;
            }
        }
        
        return null;   
    }
    
    public ResultSetFuture findOwnLocks() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_GET_BY_INSTANCE_ID));
        boundStatement.bind(instanceId);
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        return rs;
    }
    
    public List<TriggerKey> findOwnTriggersLocks() {
        
        final List<TriggerKey> ret = new LinkedList<>();

        ResultSetFuture rs = findOwnLocks(); 

        rs.getUninterruptibly().forEach(row -> {
            
            if(LockType.t.name().equals(row.getString(LOCK_TYPE))){
                ret.add(toTriggerKey(row));
            }
        });
        
        return ret;
    }
    

    public void lockUpdate(Key key, String type){
    
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_UPDATE));
        boundStatement.bind(instanceId, clock.now(), key.getName(), key.getGroup(), type);
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }
    
    public void lockJob(JobDetail job) {
        //LOG.debug("Inserting lock for job {}", job.getKey());
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_INSERT));
        boundStatement.bind(job.getKey().getName(), job.getKey().getGroup(), job.getKey().getGroup(), LockType.j.name(), instanceId, clock.now());
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void lockTrigger(TriggerKey key) {
        //LOG.info("Inserting lock for trigger {}", key);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_INSERT));
        boundStatement.bind(key.getName(), key.getGroup(), key.getGroup(), LockType.t.name(), instanceId, clock.now());
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
        
        boolean ret;

        try{
            
            Row trigerLock = findTriggerLockByTime(key, lockTime);
        
            if(trigerLock != null){

                lockTrigger(key);

                ret = true;
                
            }else{
                ret = false;
            }
            
            //LOG.info("Scheduler {} couldn't relock the trigger {} with lock time: {}",
            //    instanceId, key, lockTime);
                    
            return ret;
        } catch (CassandraDatabaseException e){
            LOG.error("Relock failed because: " + e.getMessage(), e);
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

        boolean wasApplied = false;
        
        try{
            
            List<Row> allLocks = findAllLocks(key);
            
            for (Row lock : allLocks) {
                if(instanceId.equals(lock.getString(LOCK_INSTANCE_ID))){
                
                    lockUpdate(key, lock.getString(LOCK_TYPE));
                    
                    wasApplied = true;
                }
            }
                     
        } catch (CassandraDatabaseException e){
            LOG.error("Relock failed because: " + e.getMessage(), e);
            return false;
        }
        
        return wasApplied;
    }

    public void remove(Row lock) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_DELETE));
        boundStatement.bind(lock.getString(KEY_NAME), lock.getString(KEY_GROUP), lock.getString(LOCK_TYPE));
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    /**
     * Unlock the trigger if it still belongs to the current scheduler.
     *
     * @param trigger    to unlock
     */
    public void unlockTrigger(OperableTrigger trigger) {
//        LOG.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        
         List<Row> allLocks = findAllLocks(new Key(trigger.getKey().getName(), trigger.getKey().getGroup()));
        
         allLocks.stream().filter((lock) -> (instanceId.equals(lock.getString(LOCK_INSTANCE_ID)))).forEachOrdered((lock) -> {
             remove(lock);
        });
        
//        LOG.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }

    public void unlockJob(JobDetail job) {
//        LOG.debug("Removing lock for job {}", job.getKey());
        remove(job.getKey(), LockType.j);
    }

    private void remove(JobKey jobKey, LockType lockType) {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(LOCKS_DELETE));
        boundStatement.bind(jobKey.getName(), jobKey.getGroup(), lockType.name());
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
