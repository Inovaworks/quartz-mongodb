package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PausedJobGroupsDao implements GroupedDao{

    private static final Logger log = LoggerFactory.getLogger(PausedJobGroupsDao.class);

    public static final String TABLE_NAME_PAUSED_JOB_GROUPS = "paused_job_groups";
        
    public static final String PAUSED_JOB_GROUPS_GET_ALL = CassandraConnectionManager.registerStatement ("PAUSED_JOB_GROUPS_GET_ALL", 
             "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_INSERT = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_INSERT",
            "INSERT INTO " + TABLE_NAME_PAUSED_JOB_GROUPS + " (" + KEY_GROUP + ") VALUES ("
            + "?)"
    );
    
    public static final String PAUSED_JOB_GROUPS_DELETE_ALL = CassandraConnectionManager.registerStatement ("PAUSED_JOB_GROUPS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_DELETE_MANY = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_DELETE",
            "DELETE FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
            + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_PAUSED_JOB_GROUPS
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_PAUSED_JOB_GROUPS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
    
    public PausedJobGroupsDao() {}

    public HashSet<String> getPausedGroups() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_GET_ALL));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        HashSet<String> ret =  new HashSet<>();
         rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;
    }

    public void pauseGroups(List<String> groups) {
        if (groups == null) {
            throw new IllegalArgumentException("groups cannot be null!");
        }
        
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        
        groups.stream().map((s) -> {
            BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_INSERT));
            boundStatement.bind(s);
            return boundStatement;
        }).forEachOrdered((boundStatement) -> {
            batchStatement.add(boundStatement);
        });
        
        CassandraConnectionManager.getInstance().execute(batchStatement); 
    }

    public void remove() {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_DELETE_ALL));
        CassandraConnectionManager.getInstance().executeAsync(boundStatement); 
    }

    public void unpauseGroups(Collection<String> groups) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_DELETE_MANY));
        boundStatement.bind(groups);
        CassandraConnectionManager.getInstance().executeAsync(boundStatement); 
    }
    
    @Override
    public Set<String> groupsLike(String value) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP_LIKE));
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

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_GET_BY_KEY_GROUP));
        boundStatement.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        Set<String> ret = new HashSet<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_JOB_GROUPS_GET_DISTINCT_KEY_GROUP));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret; 
    }
}
