package com.inovaworkscc.quartz.cassandra.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

import com.inovaworkscc.quartz.cassandra.db.CassandraConnectionManager;
import com.inovaworkscc.quartz.cassandra.util.SerialUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import java.util.List;

public class CalendarDao {

    private static final String ALLOW_FILTERING = " ALLOW FILTERING";
    public static final String TABLE_NAME_CALENDARS = "calendars";

    static final String CALENDAR_NAME = "name";
    static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";
    
    public static final String CALENDARS_GET_ALL = CassandraConnectionManager.registerStatement ("CALENDARS_GET_ALL", 
             "SELECT * FROM " + TABLE_NAME_CALENDARS + ALLOW_FILTERING
    );
    
    public static final String CALENDARS_GET_ALL_NAMES = CassandraConnectionManager.registerStatement ("CALENDARS_GET_ALL_NAMES", 
             "SELECT " + CALENDAR_NAME + " FROM " + TABLE_NAME_CALENDARS + ALLOW_FILTERING
    );
    
    public static final String CALENDARS_GET = CassandraConnectionManager.registerStatement("CALENDARS_GET",
            "SELECT * FROM " + TABLE_NAME_CALENDARS + " WHERE "
            + CALENDAR_NAME + " = ?" + ALLOW_FILTERING
    );
    
    public static final String CALENDARS_INSERT = CassandraConnectionManager.registerStatement("CALENDARS_INSERT",
            "INSERT INTO " + TABLE_NAME_CALENDARS + " (" + CALENDAR_NAME + ", " + CALENDAR_SERIALIZED_OBJECT + ") VALUES ("
            + "?, "
            + "?)"
    );
    
    public static final String CALENDARS_DELETE_ALL = CassandraConnectionManager.registerStatement ("CALENDARS_DELETE_ALL", 
            "TRUNCATE " + TABLE_NAME_CALENDARS
    );
    
    public static final String CALENDARS_DELETE = CassandraConnectionManager.registerStatement("CALENDARS_DELETE",
            "DELETE FROM " + TABLE_NAME_CALENDARS + " WHERE "
            + CALENDAR_NAME + " = ?" + ALLOW_FILTERING
    );
    
    public static final String CALENDARS_COUNT = CassandraConnectionManager.registerStatement("CALENDARS_COUNT",
            "SELECT COUNT(*) FROM " + TABLE_NAME_CALENDARS + ALLOW_FILTERING
    );
    
    public CalendarDao() {}

    public void clear() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_DELETE_ALL));
        CassandraConnectionManager.getInstance().execute(boundStatement);   
    }

    public long getCount() {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_COUNT));
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 

        Row r = rs.one();
        
        return r.getLong("count");
    }

    public boolean remove(String name) {
      
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_DELETE));
        boundStatement.bind(name);
        
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement); 
        return rs.one() != null;
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        
        if(calName == null)
            return null;
        
//        try {
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_GET));
        boundStatement.bind(calName);

//            ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement);
        ResultSet rs = CassandraConnectionManager.getInstance().execute(boundStatement);
        Calendar calendar;
            
//            Row r = rs.get().one();
        Row r = rs.one();

        if (r != null){

            ByteBuffer bb = r.getBytes("serializedObject");

            calendar = SerialUtils.deserialize(bb.array(), Calendar.class);

            return calendar;
        }
//        } catch (InterruptedException ex) {
//            throw new JobPersistenceException(ex.getMessage(), ex);
//        } catch (ExecutionException ex) {
//            throw new JobPersistenceException(ex.getMessage(), ex);
//        }
        
        
        return null;
    }

    public void store(String name, Calendar calendar) throws JobPersistenceException {
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_GET));
        boundStatement.bind(name, ByteBuffer.wrap(SerialUtils.serialize(calendar)));
        CassandraConnectionManager.getInstance().execute(boundStatement);           
    }

    public List<String> retrieveCalendarNames() {
        
        List<String> ret = new ArrayList<>();
        
        BoundStatement boundStatement = new BoundStatement(CassandraConnectionManager.getInstance().getStatement(CALENDARS_GET_ALL_NAMES));
        ResultSetFuture rs = CassandraConnectionManager.getInstance().executeAsync(boundStatement); 
        
        rs.getUninterruptibly().forEach(row -> {
            ret.add(row.getString("name"));
        });
                
        return ret;
    }
}
