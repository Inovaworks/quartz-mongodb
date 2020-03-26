/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inovaworkscc.quartz.cassandra.db;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author andreassilva
 */
public class CassandraDatabaseException extends RuntimeException {

    private static final String TITLE = "Database error.";
    private static final String NO_HOSTS_AVAILABLE_MSG = " Database is unreachable.";
    private static final String NO_ELEMENT_MSG = " Could not find element.";
    private static final String ERROR_IN_QUERY_MSG = " Error executing query";
    private static final String TIMEOUT_MSG = " Timeout executing query";
    public static final long serialVersionUID = -3992814505157992926L;

    public CassandraDatabaseException(InvalidQueryException ivq) {
        super(ivq);
    }

    public CassandraDatabaseException(NoHostAvailableException nhaex) {
        super(nhaex);
    }

    public CassandraDatabaseException(NoSuchElementException nseex) {
        super(nseex);
    }

    public CassandraDatabaseException(String title, String message) {
        super(TITLE);
    }
    
    public CassandraDatabaseException(TimeoutException nhaex) {
        super(nhaex);
    }

    public CassandraDatabaseException(String message) {
        super(message);
    }
}
