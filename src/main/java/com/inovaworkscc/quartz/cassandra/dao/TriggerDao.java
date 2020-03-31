package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.Constants;
import com.inovaworkscc.quartz.cassandra.JobConverter;
import com.inovaworkscc.quartz.cassandra.trigger.TriggerConverter;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.trigger.properties.CalendarIntervalTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.CronTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.DailyTimeIntervalTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.trigger.properties.SimpleTriggerPropertiesConverter;
import com.inovaworkscc.quartz.cassandra.util.Keys;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_NAME;
import com.inovaworkscc.quartz.cassandra.util.QueryHelper;
import java.util.Collection;
import java.util.HashMap;

public class TriggerDao implements GroupedDao{

    public static final String TABLE_NAME_TRIGGERS = "triggers";

    public static final String TRIGGERS_GET_ALL = CassandraConnectionManager.registerStatement ("TRIGGERS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_TRIGGERS
    );
    
    
    public static final String TRIGGERS_DELETE_ALL = CassandraConnectionManager.registerStatement ("TRIGGERS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_TRIGGERS
    );
    
    public static final String TRIGGERS_COUNT = CassandraConnectionManager.registerStatement("TRIGGERS_COUNT",
            "SELECT COUNT(*) FROM " + TABLE_NAME_TRIGGERS
    );
    
    public static final String TRIGGERS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("TRIGGERS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_TRIGGERS
    );
    
    public static final String TRIGGERS_GET_KEY_NAME = CassandraConnectionManager.registerStatement("TRIGGERS_GET_KEY_NAME",
            "SELECT " + KEY_NAME + " FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_GET_BY_KEY = CassandraConnectionManager.registerStatement("TRIGGERS_GET_BY_KEY",
            "SELECT * FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_GET_BY_JOB_ID = CassandraConnectionManager.registerStatement("TRIGGERS_GET_BY_JOB_ID",
            "SELECT * FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + Constants.TRIGGER_JOB_ID + " = ?"
    );
    public static final String TRIGGERS_GET_KEY_BY_JOB_ID = CassandraConnectionManager.registerStatement("TRIGGERS_GET_KEY_BY_JOB_ID",
            "SELECT " + KEY_NAME + "," + KEY_GROUP + "FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + Constants.TRIGGER_JOB_ID + " = ?"
    );
    
    public static final String TRIGGERS_GET_KEY_IN_JOB_ID = CassandraConnectionManager.registerStatement("TRIGGERS_GET_KEY_IN_JOB_ID",
            "SELECT " + KEY_NAME + "," + KEY_GROUP + "FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + Constants.TRIGGER_JOB_ID + " IN ?"
    );
    
    public static final String TRIGGERS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("TRIGGERS_GET_BY_KEY_GROUP_LIKE",
            "SELECT " + KEY_NAME + "," + KEY_GROUP + " FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
    
    public static final String TRIGGERS_INSERT = CassandraConnectionManager.registerStatement("TRIGGERS_INSERT",
            "INSERT INTO " + TABLE_NAME_TRIGGERS + " (" 
                    + KEY_NAME + "," 
                    + KEY_GROUP + "," 
                    + KEY_GROUP + "_index" + "," 
                    + Constants.TRIGGER_STATE + "," 
                    + TriggerConverter.TRIGGER_CLASS + "," 
                    + TriggerConverter.TRIGGER_CALENDAR_NAME + "," 
                    + TriggerConverter.TRIGGER_DESCRIPTION + "," 
                    + Constants.TRIGGER_JOB_ID  + "," 
                    + Constants.JOB_DATA  + "," 
                    + Constants.JOB_DATA_PLAIN  + "," 
                    + TriggerConverter.TRIGGER_PRIORITY  + "," 
                    + TriggerConverter.TRIGGER_START_TIME  + "," 
                    + TriggerConverter.TRIGGER_END_TIME  + "," 
                    + CronTriggerPropertiesConverter.TRIGGER_CRON_EXPRESSION + "," 
                    + TriggerConverter.TRIGGER_FIRE_INSTANCE_ID + "," 
                    + TriggerConverter.TRIGGER_PREVIOUS_FIRE_TIME + "," 
                    + Constants.TRIGGER_NEXT_FIRE_TIME + "," 
                    + TriggerConverter.TRIGGER_FINAL_FIRE_TIME + "," 
                    + TriggerConverter.TRIGGER_MISFIRE_INSTRUCTION + "," 
                    + CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL_UNIT + "," 
                    + CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL + "," 
                    + CalendarIntervalTriggerPropertiesConverter.TRIGGER_TIMES_TRIGGERED + "," 
                    + CronTriggerPropertiesConverter.TRIGGER_TIMEZONE + "," 
                    + DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_START_TIME_OF_DAY + "," 
                    + DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_END_TIME_OF_DAY + "," 
                    + SimpleTriggerPropertiesConverter.TRIGGER_REPEAT_COUNT
                    + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    public static final String TRIGGERS_UPSERT = CassandraConnectionManager.registerStatement("TRIGGERS_UPSERT",
            "UPDATE " + TABLE_NAME_TRIGGERS + " SET " 
                + Constants.TRIGGER_STATE + " = ? ,"
                + TriggerConverter.TRIGGER_CLASS + " = ? ,"
                + TriggerConverter.TRIGGER_CALENDAR_NAME + " = ? ,"
                + TriggerConverter.TRIGGER_DESCRIPTION + " = ? ,"
                + Constants.TRIGGER_JOB_ID + " = ? ,"
                + Constants.JOB_DATA + " = ? ,"
                + Constants.JOB_DATA_PLAIN + " = ? ,"
                + TriggerConverter.TRIGGER_PRIORITY + " = ? ,"
                + TriggerConverter.TRIGGER_START_TIME + " = ? ,"
                + TriggerConverter.TRIGGER_END_TIME + " = ? ,"
                + CronTriggerPropertiesConverter.TRIGGER_CRON_EXPRESSION + " = ? ,"
                + TriggerConverter.TRIGGER_FIRE_INSTANCE_ID + " = ? ,"
                + TriggerConverter.TRIGGER_PREVIOUS_FIRE_TIME + " = ? ,"
                + Constants.TRIGGER_NEXT_FIRE_TIME + " = ? ,"
                + TriggerConverter.TRIGGER_FINAL_FIRE_TIME + " = ? ,"
                + TriggerConverter.TRIGGER_MISFIRE_INSTRUCTION + " = ? ,"
                + CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL_UNIT + " = ? ,"
                + CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL + " = ? ,"
                + CalendarIntervalTriggerPropertiesConverter.TRIGGER_TIMES_TRIGGERED + " = ? ,"
                + CronTriggerPropertiesConverter.TRIGGER_TIMEZONE + " = ? ,"
                + DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_START_TIME_OF_DAY + " = ? ,"
                + DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_END_TIME_OF_DAY + " = ? ,"
                + SimpleTriggerPropertiesConverter.TRIGGER_REPEAT_COUNT + " = ? "
                + "WHERE "
                + KEY_NAME + " = ? AND "
                + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_UPDATE_STATE = CassandraConnectionManager.registerStatement("TRIGGERS_UPDATE_STATE",
            "UPDATE " + TABLE_NAME_TRIGGERS + " SET " 
                + Constants.TRIGGER_STATE + " = ? "
                + "WHERE "
                + KEY_NAME + " = ? AND "
                + KEY_GROUP + " = ? "
                + "IF EXISTS"
    );
    
    public static final String TRIGGERS_UPSERT_STATE = CassandraConnectionManager.registerStatement("TRIGGERS_UPSERT_STATE",
            "UPDATE " + TABLE_NAME_TRIGGERS + " SET " 
                + Constants.TRIGGER_STATE + " = ? "
                + "WHERE "
                + KEY_NAME + " = ? AND "
                + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_UPDATE_STATE_IN_NAME = CassandraConnectionManager.registerStatement("TRIGGERS_UPDATE_STATE_IN_NAME",
            "UPDATE " + TABLE_NAME_TRIGGERS + " SET " 
                + Constants.TRIGGER_STATE + " = ? "
                + "WHERE "
                + KEY_NAME + " IN ? AND "
                + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_DELETE_BY_KEY = CassandraConnectionManager.registerStatement("TRIGGERS_DELETE_BY_KEY",
            "DELETE FROM " + TABLE_NAME_TRIGGERS + " WHERE "
            + KEY_NAME + " = ? AND "
            + KEY_GROUP + " = ?"
    );
    
    public static final String TRIGGERS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("TRIGGERS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_TRIGGERS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    private QueryHelper queryHelper;
    private TriggerConverter triggerConverter;

    public TriggerDao(QueryHelper queryHelper,
                      TriggerConverter triggerConverter) {
        this.queryHelper = queryHelper;
        this.triggerConverter = triggerConverter;
    }

    public void clear() {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_DELETE_ALL));
        CassandraConnectionManager.getInstance().execute(boundStatement);   
    }

    public boolean exists(TriggerKey key) throws JobPersistenceException {
        
        OperableTrigger existingTrigger = this.getTrigger(key);
        
        return existingTrigger != null;      
    }

    public List<Row> findEligibleToRun(Date noLaterThanDate) {
        
        List<Row> ret = new ArrayList<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_ALL));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        rs.getUninterruptibly().forEach(row -> {
            Date triggerNextFireTime = row.getTimestamp(Constants.TRIGGER_NEXT_FIRE_TIME);
            String triggerState = row.getString(Constants.TRIGGER_STATE);
            
            if ((triggerNextFireTime == null || triggerNextFireTime.before(noLaterThanDate) || triggerNextFireTime.equals(noLaterThanDate)) 
                    && Constants.STATE_WAITING.equals(triggerState)){
                
                ret.add(row);
            }
        });
        
        ret.sort((Row o1, Row o2) -> o1.getTimestamp(Constants.TRIGGER_NEXT_FIRE_TIME).compareTo(o2.getTimestamp(Constants.TRIGGER_NEXT_FIRE_TIME)));

        return ret;
    }

    public Row findTrigger(TriggerKey triggerKey) {
                
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY));
        boundStatement.bind(triggerKey.getName(), triggerKey.getGroup());
        Row row = CassandraConnectionManager.getInstance().execute(boundStatement).one();

        return row;
    }
    
    public long getCount() {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_COUNT));
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r.getLong("count");
    }

    public List<String> getGroupNames() {

        List<String> ret = new ArrayList<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_DISTINCT_KEY_GROUP));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;
    }
    
    public List<String> getGroupNamesName(String keyGroup) {

        List<String> ret = new ArrayList<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_KEY_NAME));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_NAME));
        });
        
        return ret;
    }

    public String getState(TriggerKey triggerKey) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY));
        boundStatement.bind(triggerKey.getName(), triggerKey.getGroup());
        Row row = CassandraConnectionManager.getInstance().execute(boundStatement).one();

        return row.getString(Constants.TRIGGER_STATE);
    }

    public OperableTrigger getTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY));
        boundStatement.bind(triggerKey.getName(), triggerKey.getGroup());
        Row row = CassandraConnectionManager.getInstance().execute(boundStatement).one();

        if (row == null) {
            return null;
        }
        return triggerConverter.toTrigger(triggerKey, row);
    }

    public List<OperableTrigger> getTriggersForJob(Row job) throws JobPersistenceException {
        final List<OperableTrigger> triggers = new LinkedList<OperableTrigger>();
        if (job != null) {
            for (Row item : findByJobId(job.getString(JobConverter.JOB_ID))) {
                triggers.add(triggerConverter.toTrigger(item));
            }
        }
        return triggers;
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) {
        
        Set<TriggerKey> ret = new HashSet<>();
        
        String value = queryHelper.matchingKeysConditionForCassandra(matcher);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY_GROUP_LIKE));
        boundStatement.bind(value);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 
                        
        for (Row row : rs.all()) {
            ret.add(Keys.toTriggerKey(row));
        }
        return ret;
    }

    public boolean hasLastTrigger(Row job) {
        
        List<Row> findByJobId = findByJobId(job.getString(JobConverter.JOB_ID));
        
        return findByJobId.size() == 1;
    }

    public void insert( HashMap<String, Object> trigger, Trigger offendingTrigger)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        OperableTrigger existingTrigger = this.getTrigger(offendingTrigger.getKey());
        if (existingTrigger == null) {
          
            BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_INSERT));
            boundStatement.bind(
                    trigger.get(KEY_NAME),
                    trigger.get(KEY_GROUP),
                    trigger.get(KEY_GROUP + "_index"),
                    trigger.get(Constants.TRIGGER_STATE),
                    trigger.get(TriggerConverter.TRIGGER_CLASS),
                    trigger.get(TriggerConverter.TRIGGER_CALENDAR_NAME),
                    trigger.get(TriggerConverter.TRIGGER_DESCRIPTION),
                    trigger.get(Constants.TRIGGER_JOB_ID),
                    trigger.get(Constants.JOB_DATA),
                    trigger.get(Constants.JOB_DATA_PLAIN),
                    trigger.get(TriggerConverter.TRIGGER_PRIORITY),
                    trigger.get(TriggerConverter.TRIGGER_START_TIME),
                    trigger.get(TriggerConverter.TRIGGER_END_TIME),
                    trigger.get(CronTriggerPropertiesConverter.TRIGGER_CRON_EXPRESSION),
                    trigger.get(TriggerConverter.TRIGGER_FIRE_INSTANCE_ID),
                    trigger.get(TriggerConverter.TRIGGER_PREVIOUS_FIRE_TIME),
                    trigger.get( Constants.TRIGGER_NEXT_FIRE_TIME),
                    trigger.get(TriggerConverter.TRIGGER_FINAL_FIRE_TIME),
                    trigger.get(TriggerConverter.TRIGGER_MISFIRE_INSTRUCTION),
                    trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL_UNIT),
                    trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL),
                    trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_TIMES_TRIGGERED),
                    trigger.get(CronTriggerPropertiesConverter.TRIGGER_TIMEZONE),
                    trigger.get(DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_START_TIME_OF_DAY),
                    trigger.get(DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_END_TIME_OF_DAY),
                    trigger.get(SimpleTriggerPropertiesConverter.TRIGGER_REPEAT_COUNT)
            );
            
            CassandraConnectionManager.getInstance().execute(boundStatement); 
            
        }else{
            throw new ObjectAlreadyExistsException(offendingTrigger);
        }
    }


    public void remove(TriggerKey triggerKey) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_DELETE_BY_KEY));
        boundStatement.bind(
            triggerKey.getName(),
            triggerKey.getGroup());
        
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void removeByJobId(Object id) {
        
        List<Row> triggersToDelete = findKeyByJobId(id.toString());
        for (Row row : triggersToDelete) {
            remove(new TriggerKey(row.getString(KEY_NAME), row.getString(KEY_GROUP)));
        }
    }

    public void replace(TriggerKey triggerKey, HashMap<String, Object> trigger) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_UPSERT));
        boundStatement.bind(
                trigger.get(Constants.TRIGGER_STATE),
                trigger.get(TriggerConverter.TRIGGER_CLASS),
                trigger.get(TriggerConverter.TRIGGER_CALENDAR_NAME),
                trigger.get(TriggerConverter.TRIGGER_DESCRIPTION),
                trigger.get(Constants.TRIGGER_JOB_ID),
                trigger.get(Constants.JOB_DATA),
                trigger.get(Constants.JOB_DATA_PLAIN),
                trigger.get(TriggerConverter.TRIGGER_PRIORITY),
                trigger.get(TriggerConverter.TRIGGER_START_TIME),
                trigger.get(TriggerConverter.TRIGGER_END_TIME),
                trigger.get(CronTriggerPropertiesConverter.TRIGGER_CRON_EXPRESSION),
                trigger.get(TriggerConverter.TRIGGER_FIRE_INSTANCE_ID),
                trigger.get(TriggerConverter.TRIGGER_PREVIOUS_FIRE_TIME),
                trigger.get(Constants.TRIGGER_NEXT_FIRE_TIME),
                trigger.get(TriggerConverter.TRIGGER_FINAL_FIRE_TIME),
                trigger.get(TriggerConverter.TRIGGER_MISFIRE_INSTRUCTION),
                trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL_UNIT),
                trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_REPEAT_INTERVAL),
                trigger.get(CalendarIntervalTriggerPropertiesConverter.TRIGGER_TIMES_TRIGGERED),
                trigger.get(CronTriggerPropertiesConverter.TRIGGER_TIMEZONE),
                trigger.get(DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_START_TIME_OF_DAY),
                trigger.get(DailyTimeIntervalTriggerPropertiesConverter.TRIGGER_END_TIME_OF_DAY),
                trigger.get(SimpleTriggerPropertiesConverter.TRIGGER_REPEAT_COUNT),
                triggerKey.getName(),
                triggerKey.getGroup()
        );

        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void setState(TriggerKey triggerKey, String state, boolean upsert) {
        
        BoundStatement boundStatement;
        
        if (upsert) {
            boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_UPSERT_STATE));
        }else {
            boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_UPDATE_STATE));
        }
        
        boundStatement.bind(
            state,
            triggerKey.getName(),
            triggerKey.getGroup()
        );

        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void transferState(TriggerKey triggerKey, String oldState, String newState) {
        
        String currentState = getState(triggerKey);
        if(oldState.equals(currentState)){
            setState(triggerKey, newState, false);
        }
    }

    public void setStateInAll(String state) {
        
        List<String> groupNames = getGroupNames();
        
        setStates(groupNames, state);
    }

    public void setStateByJobId(String jobId, String state) {
        
        List<Row> foundJob = findByJobId(jobId);
        
        foundJob.forEach((row) -> {
            setState(new TriggerKey(row.getString(KEY_NAME), row.getString(KEY_GROUP)), state, false);
        });
    }

    public void setStateInGroups(List<String> groups, String state) {
        setStates(groups, state);
    }

    public void setStateInMatching(GroupMatcher<TriggerKey> matcher, String state) {
        setStates(matcher, state);
    }

    private List<Row> findByJobId(String jobId) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_JOB_ID));
        boundStatement.bind(jobId);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);

        return rs.all();
    }
    
    public List<Row> findKeyByJobId(String jobId) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_KEY_BY_JOB_ID));
        boundStatement.bind(jobId);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);

        return rs.all();
    }
    
    public List<Row> findKeyByJobId(Collection<String> jobId) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_KEY_IN_JOB_ID));
        boundStatement.bind(jobId);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);

        return rs.all();
    }

   
    private void setStates(List<String> groups, String state) {

        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        
        groups.stream().map((groupName) -> {

            List<String> groupNamesName = getGroupNamesName(groupName);

            BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_UPDATE_STATE_IN_NAME));
            boundStatement.bind(
                    state,
                    groupNamesName,
                    groupName
            );
            
            return boundStatement;
            
        }).forEachOrdered((boundStatement) -> {
            batchStatement.add(boundStatement);
        });
        
        CassandraConnectionManager.getInstance().execute(batchStatement);
    }

    private void setStates(GroupMatcher<TriggerKey> matcher, String state) {
        
        Set<TriggerKey> triggerKeys = getTriggerKeys(matcher);
        
        triggerKeys.forEach((triggerKey) -> {
            setState(triggerKey, state, false);
        });
    }
    
    @Override
    public Set<String> groupsLike(String value) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY_GROUP_LIKE));
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

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(TRIGGERS_GET_BY_KEY_GROUP));
        boundStatement.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        return new HashSet<>(getGroupNames()); 
    }
}
