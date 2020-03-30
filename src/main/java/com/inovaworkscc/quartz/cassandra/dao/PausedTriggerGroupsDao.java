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

public class PausedTriggerGroupsDao implements GroupedDao{

    public static final String TABLE_NAME_PAUSED_TRIGGER_GROUPS = "paused_trigger_groups";
        
    public static final String PAUSED_TRIGGER_GROUPS_GET_ALL = CassandraConnectionManager.registerStatement ("PAUSED_TRIGGER_GROUPS_GET_ALL", 
            "SELECT * FROM " + TABLE_NAME_PAUSED_TRIGGER_GROUPS
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_INSERT = CassandraConnectionManager.registerStatement("PAUSED_TRIGGER_GROUPS_INSERT",
            "INSERT INTO " + TABLE_NAME_PAUSED_TRIGGER_GROUPS + " (" + KEY_GROUP + ") VALUES ("
            + "?)"
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_DELETE_ALL = CassandraConnectionManager.registerStatement ("PAUSED_TRIGGER_GROUPS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_PAUSED_TRIGGER_GROUPS
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_DELETE_MANY = CassandraConnectionManager.registerStatement("PAUSED_TRIGGER_GROUPS_DELETE_MANY",
            "DELETE FROM " + TABLE_NAME_PAUSED_TRIGGER_GROUPS + " WHERE "
            + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_GET_DISTINCT_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_TRIGGER_GROUPS_GET_DISTINCT_KEY_GROUP",
            "SELECT DISTINCT " + KEY_GROUP + " FROM " + TABLE_NAME_PAUSED_TRIGGER_GROUPS
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP = CassandraConnectionManager.registerStatement("PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP",
            "SELECT * FROM " + TABLE_NAME_PAUSED_TRIGGER_GROUPS + " WHERE "
                    + KEY_GROUP + " IN ?"
    );
    
    public static final String PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP_LIKE = CassandraConnectionManager.registerStatement("PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP_LIKE",
            "SELECT * FROM " + TABLE_NAME_PAUSED_TRIGGER_GROUPS + " WHERE "
                    + KEY_GROUP + "_index LIKE ?"
    );
    
    public PausedTriggerGroupsDao() {}

    public HashSet<String> getPausedGroups() {
        //return triggerGroupsCollection.distinct(KEY_GROUP, String.class).into(new HashSet<String>());
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_GET_ALL));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 

        HashSet<String> ret =  new HashSet<>();
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret;     
    }

    public void pauseGroups(Collection<String> groups) {
        
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        
        groups.stream().map((s) -> {
            BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_INSERT));
            boundStatement.bind(s);
            return boundStatement;
        }).forEachOrdered((boundStatement) -> {
            batchStatement.add(boundStatement);
        });
        
        CassandraConnectionManager.getInstance().execute(batchStatement); 
    }

    public void remove() {

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_DELETE_ALL));
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }

    public void unpauseGroups(Collection<String> groups) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_DELETE_MANY));
        boundStatement.bind(groups);
        CassandraConnectionManager.getInstance().execute(boundStatement); 
    }
    
    @Override
    public Set<String> groupsLike(String value) {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP_LIKE));
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

        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_GET_BY_KEY_GROUP));
        boundStatement.bind(groups);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        return rs.all();
    }

    @Override
    public Set<String> allGroups() {

        Set<String> ret = new HashSet<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(PAUSED_TRIGGER_GROUPS_GET_DISTINCT_KEY_GROUP));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString(KEY_GROUP));
        });
        
        return ret; 
    }
}
