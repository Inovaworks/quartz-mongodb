package com.inovaworkscc.quartz.cassandra.util;

import org.quartz.impl.matchers.GroupMatcher;

public class QueryHelper {

    public String matchingKeysConditionForCassandra(GroupMatcher<?> matcher) {
        final String compareToValue = matcher.getCompareToValue();

        switch (matcher.getCompareWithOperator()) {
            case EQUALS:
                return compareToValue;
            case STARTS_WITH:
                return compareToValue + "%";
            case ENDS_WITH:
                return "%" + compareToValue;
            case CONTAINS:
                return "%" + compareToValue + "%";
        }

        return "";
    }
}
