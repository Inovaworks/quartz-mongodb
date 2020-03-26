package com.inovaworkscc.quartz.cassandra;

import com.datastax.driver.core.Row;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_GROUP;
import static com.inovaworkscc.quartz.cassandra.util.Keys.KEY_NAME;
import org.quartz.*;
import org.quartz.spi.ClassLoadHelper;

public class JobConverter {

    public static final String JOB_ID = "jobId";
    public static final String JOB_DURABILITY = "durability";
    public static final String JOB_CLASS = "jobClass";
    public static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_REQUESTS_RECOVERY = "requestsRecovery";

    private ClassLoadHelper loadHelper;
    public final JobDataConverter jobDataConverter;

    public JobConverter(ClassLoadHelper loadHelper, JobDataConverter jobDataConverter) {
        this.loadHelper = loadHelper;
        this.jobDataConverter = jobDataConverter;
    }

    /**
     * Converts from row to job detail.
     */
    public JobDetail toJobDetail(Row row) throws JobPersistenceException {
        try {
            // Make it possible for subclasses to use custom class loaders.
            // When Quartz jobs are implemented as Clojure records, the only way to use
            // them without switching to gen-class is by using a
            // clojure.lang.DynamicClassLoader instance.
            @SuppressWarnings("unchecked")
            Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader()
                    .loadClass(row.getString(JOB_CLASS));

            JobBuilder builder = createJobBuilder(row, jobClass);
            withDurability(row, builder);
            withRequestsRecovery(row, builder);
            JobDataMap jobData = createJobDataMap(row);
            return builder.usingJobData(jobData).build();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not load job class " + row.getString(JOB_CLASS), e);
        }
    }

    /**
     * Converts row into job data map.
     * Will first try {@link JobDataConverter} to deserialize
     * from '{@value Constants#JOB_DATA}' ({@code base64})
     * or '{@value Constants#JOB_DATA_PLAIN}' fields.
     * If didn't succeed, will try to build job data
     * from root fields (legacy, subject to remove).
     */
    private JobDataMap createJobDataMap(Row row) throws JobPersistenceException {
        JobDataMap jobData = new JobDataMap();

        if (!jobDataConverter.toJobData(row, jobData)) {
            
            if(!row.isNull(Constants.JOB_DATA)){
            
                jobData.put(Constants.JOB_DATA, row.getString(Constants.JOB_DATA));
            }
            
            if(!row.isNull(Constants.JOB_DATA_PLAIN)){
            
                jobData.put(Constants.JOB_DATA_PLAIN, row.getString(Constants.JOB_DATA_PLAIN));
            }
        }

        jobData.clearDirtyFlag();
        return jobData;
    }

    private void withDurability(Row row, JobBuilder builder) throws JobPersistenceException {
        Object jobDurability = row.getObject(JOB_DURABILITY);
        if (jobDurability != null) {
            if (jobDurability instanceof Boolean) {
                builder.storeDurably((Boolean) jobDurability);
            } else if (jobDurability instanceof String) {
                builder.storeDurably(Boolean.valueOf((String) jobDurability));
            } else {
                throw new JobPersistenceException("Illegal value for " + JOB_DURABILITY + ", class "
                        + jobDurability.getClass() + " not supported");
            }
        }
    }

    private void withRequestsRecovery(Row row, JobBuilder builder) {
        if (row.getBool(JOB_REQUESTS_RECOVERY)) {
            builder.requestRecovery(true);
        }
    }

    private JobBuilder createJobBuilder(Row row, Class<Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(row.getString(KEY_NAME), row.getString(KEY_GROUP))
                .withDescription(row.getString(JOB_DESCRIPTION));
    }
}
