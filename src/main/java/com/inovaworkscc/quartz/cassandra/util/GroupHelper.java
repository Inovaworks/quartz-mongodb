package com.inovaworkscc.quartz.cassandra.util;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.dao.GroupedDao;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.List;
import java.util.Set;

public class GroupHelper {
    protected GroupedDao dao;
    protected QueryHelper queryHelper;

    public GroupHelper(GroupedDao dao, QueryHelper queryHelper) {
        this.dao = dao;
        this.queryHelper = queryHelper;
    }

    public Set<String> groupsThatMatch(GroupMatcher<?> matcher) {
        String filter = queryHelper.matchingKeysConditionForCassandra(matcher);
        
        return dao.groupsLike(filter);
    }

    public List<Row> inGroupsThatMatch(GroupMatcher<?> matcher) {
        
        Set<String> groupsLike = groupsThatMatch(matcher);
                
        return dao.rowsInGroups(groupsLike);
    }

    public Set<String> allGroups() {
        
        return dao.allGroups();
    }
}
