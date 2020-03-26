package com.inovaworkscc.quartz.cassandra.util;

import com.datastax.driver.core.Row;
import com.inovaworkscc.quartz.cassandra.dao.TriggerDao;

import java.util.Collection;
import java.util.List;

import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TriggerGroupHelper extends GroupHelper {
  public static final String JOB_ID = "jobId";

  public TriggerGroupHelper(TriggerDao dao, QueryHelper queryHelper) {
    super(dao, queryHelper);
  }

  public List<String> groupsForJobId(String jobId) {
      
    Set<String> set = new HashSet<>();
      
    List<Row> findKeyByJobId = ((TriggerDao)dao).findKeyByJobId(jobId);
    findKeyByJobId.forEach((row) -> {
        set.add(row.getString(KEY_GROUP));
      });

    return new ArrayList<>(set);
  }

  public List<String> groupsForJobIds(Collection<String> ids) {
    Set<String> set = new HashSet<>();
      
    List<Row> findKeyByJobId = ((TriggerDao)dao).findKeyByJobId(ids);
    findKeyByJobId.forEach((row) -> {
        set.add(row.getString(KEY_GROUP));
    });

    return new ArrayList<>(set);
  }
}
