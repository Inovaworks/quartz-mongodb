package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.JobConverter;
import static com.inovaworkscc.quartz.cassandra.JobConverter.*;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.db.CassandraDatabaseException;
import static com.inovaworkscc.quartz.cassandra.Constants.JOB_DATA;
import static com.inovaworkscc.quartz.cassandra.Constants.JOB_DATA_PLAIN;
import com.inovaworkscc.quartz.cassandra.util.Keys;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;

import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_NAME;
import com.inovaworkscc.quartz.cassandra.util.QueryHelper;
import com.inovaworkscc.quartz.cassandra.util.SerialUtils;
import java.io.IOException;
import org.quartz.JobDataMap;

public class JobDao implements GroupedDao{
    
    private static final String ALLOW_FILTERING = " ALLOW FILTERING";
    public static final String TABLE_NAME_JOBS = "jobs";

    public static final String JOBS_GET_ALL = CassandraConnectionManager.registerStatement ("JOBS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_JOBS + ALLOW_FILTERING
    );
    
    public static final String JOBS_DELETE_ALL = CassandraConnectionManager.registerStatement ("JOBS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_JOBS
    );
    
    public static final String JOBS_COUNT_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_COUNT_BY_KEY",
            "SELECT COUNT(*) FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ?" + ALLOW_FILTERING
    );
    
    public static final String JOBS_COUNT = CassandraConnectionManager.registerStatement("JOBS_COUNT",
            "SELECT COUNT(*) FROM " + TABLE_NAME_JOBS + ALLOW_FILTERING
    );
    
    public static final String JOBS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_GROUP + " IN ?" + ALLOW_FILTERING
    );
    
    public static final String JOBS_GET_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_NAME + " = ? AND "
                    + KEY_GROUP + " = ?" + ALLOW_FILTERING
    );
    
    public static final String JOBS_GET_BY_JOB_ID = CassandraConnectionManager.registerStatement("JOBS_GET_BY_JOB_ID",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + JOB_ID + " = ?" + ALLOW_FILTERING
    );
    
    public static final String JOBS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("JOBS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_JOBS + ALLOW_FILTERING
    );
    
    public static final String JOBS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("JOBS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_JOBS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?" + ALLOW_FILTERING
    );
        
    public static final String JOBS_DELETE_BY_KEY = CassandraConnectionManager.registerStatement("JOBS_DELETE_BY_KEY",
            "DELETE FROM " + TABLE_NAME_JOBS + " WHERE "
            + KEY_NAME + " = ? AND "
            + KEY_GROUP + " = ?" + ALLOW_FILTERING
    );

    public static final String JOBS_INSERT_JOB_DATA = CassandraConnectionManager.registerStatement("JOBS_INSERT_JOB_DATA",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + "," + JOB_DATA + ")" + "VALUES ("
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
    
    public static final String JOBS_INSERT_JOB_DATA_PLAIN = CassandraConnectionManager.registerStatement("JOBS_INSERT_JOB_DATA_PLAIN",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + "," + JOB_DATA_PLAIN + ")" + "VALUES ("
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
    
    public static final String JOBS_INSERT_NO_JOB_DATA_PLAIN = CassandraConnectionManager.registerStatement("JOBS_INSERT_NO_JOB_DATA_PLAIN",
            "INSERT INTO " + TABLE_NAME_JOBS + " (" + KEY_NAME + "," + KEY_GROUP + "," + KEY_GROUP + "_index" + "," + JOB_ID + "," + JOB_DESCRIPTION + "," + JOB_CLASS + "," + JOB_DURABILITY + "," + JOB_REQUESTS_RECOVERY + ")" + "VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?)"
    );
    
    private final QueryHelper queryHelper;
    private final JobConverter jobConverter;

    public JobDao(QueryHelper queryHelper, JobConverter jobConverter) {
        this.queryHelper = queryHelper;
        this.jobConverter = jobConverter;
    }

    public List<Row> clear() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_DELETE_ALL));
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);   
        
        return rs.all();
    }

    public boolean exists(JobKey jobKey) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_COUNT_BY_KEY));
        boundStatement.bind(jobKey.getName(), jobKey.getGroup());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r.getLong("count") > 0;
    }

    public Row getById(String id) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_JOB_ID));
        boundStatement.bind(id);
                
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r;
    }
    
    public Row getJob(JobKey jobKey) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_KEY));
        boundStatement.bind(jobKey.getName(), jobKey.getGroup());
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r;
    }

    public long getCount() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_COUNT));
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r.getLong("count");
    }

    public List<String> getGroupNames() {

        List<String> ret = new ArrayList<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_DISTINCT_KEY_GROUP));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;
    }

    public List<Row> getJobs(GroupMatcher<JobKey> matcher) {
        
        String value = queryHelper.matchingKeysConditionForCassandra(matcher);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_KEY_GROUP_LIKE));
        boundStatement.bind(value);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 
                
        return rs.all();
    }
    
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
        
        String value = queryHelper.matchingKeysConditionForCassandra(matcher);
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_KEY_GROUP_LIKE));
        boundStatement.bind(value);
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        Set<JobKey> keys = new HashSet<>();
        rs.getUninterruptibly().forEach(row -> {
            keys.add(Keys.toJobKey(row));
        });
                
        return keys;
    }

    public Collection<String> idsOfMatching(GroupMatcher<JobKey> matcher) {
        
        List<Row> rows = getJobs(matcher);
        
        List<String> ret = new ArrayList<>();
        rows.forEach((row) -> {
            ret.add(row.getString(JOB_ID));
        });
        return ret;
    }

    public void remove(JobKey jobKey) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_DELETE_BY_KEY));
        boundStatement.bind(jobKey.getName(), jobKey.getGroup());
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public boolean requestsRecovery(JobKey jobKey) {
        //TODO check if it's the same as getJobDataMap?
        Row row = getJob(jobKey);
        
        return row.getBool(JobConverter.JOB_REQUESTS_RECOVERY);
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        Row row = getJob(jobKey);
        if (row == null) {
            //Return null if job does not exist, per interface
            return null;
        }
        return jobConverter.toJobDetail(row);
    }

    public String storeJobInCassandra(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        JobKey key = newJob.getKey();

        Row existingJob = getJob(key);

        JobDataMap jobDataMap = newJob.getJobDataMap();

        String jobId;
        if (existingJob != null && replaceExisting) {
            
            jobId = existingJob.getString(JOB_ID);
            
            storeJob(jobDataMap, key, jobId, newJob); //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData
            //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData

        } else if (existingJob == null) {
            
            jobId = UUID.randomUUID().toString();
            
            storeJob(jobDataMap, key, jobId, newJob);
        } else {
            jobId = existingJob.getString(JOB_ID);
        }

        return jobId;
    }

    private void storeJob(JobDataMap jobDataMap, JobKey key, String jobId, JobDetail newJob) throws JobPersistenceException, CassandraDatabaseException {
        
        BoundStatement boundStatement;
        
        if(jobDataMap.isEmpty()){
            
            boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_INSERT_NO_JOB_DATA_PLAIN));
            boundStatement.bind(
                    key.getName(),
                    key.getGroup(),
                    key.getGroup(),
                    jobId,
                    newJob.getDescription(),
                    newJob.getJobClass().getName(),
                    newJob.isDurable(),
                    newJob.requestsRecovery()
            );
        }
        if (jobConverter.jobDataConverter.isBase64Preferred()){
            
            String jobDataString;
            try {
                jobDataString = SerialUtils.serialize(jobDataMap);
            } catch (IOException e) {
                throw new JobPersistenceException("Could not serialise job data.", e);
            }
            
            boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_INSERT_JOB_DATA));
            
            //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData
            boundStatement.bind(
                    key.getName(),
                    key.getGroup(),
                    key.getGroup(),
                    jobId,
                    newJob.getDescription(),
                    newJob.getJobClass().getName(),
                    newJob.isDurable(),
                    newJob.requestsRecovery(),
                    jobDataString
            );
        }else{
            
            boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_INSERT_JOB_DATA_PLAIN));
            
            //keyName, keyGroup, keyGroup_index, jobId, jobDescription, jobClass, durability, requestsRecovery, jobData
            boundStatement.bind(
                    key.getName(),
                    key.getGroup(),
                    key.getGroup(),
                    jobId,
                    newJob.getDescription(),
                    newJob.getJobClass().getName(),
                    newJob.isDurable(),
                    newJob.requestsRecovery(),
                    jobDataMap.getWrappedMap()
            );
        }
        
        CassandraConnectionManager.getInstance().execute(boundStatement);         
    }

    @Override
    public Set<String> groupsLike(String value) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_KEY_GROUP_LIKE));
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

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(JOBS_GET_BY_KEY_GROUP));
        boundStatement.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        return new HashSet<>(getGroupNames()); 
    }
}
