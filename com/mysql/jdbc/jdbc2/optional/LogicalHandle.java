/*
   Copyright (C) 2002 MySQL AB

      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.

      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.

      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

 */
package com.mysql.jdbc.jdbc2.optional;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;


/**
 * This class serves as a wrapper for the org.gjt.mm.mysql.jdbc2.Connection
 * class.   It is returned to the application server which may wrap it again
 * and then return it to the application client in response to
 * dataSource.getConnection().
 * 
 * <p>
 * All method invocations are forwarded to org.gjt.mm.mysql.jdbc2.Connection
 * unless the close method was previously called, in which case a sqlException
 * is  thrown.  The close method performs a 'logical close' on the connection.
 * </p>
 * 
 * <p>
 * All sqlExceptions thrown by the physical connection are intercepted and sent
 * to  connectionEvent listeners before being thrown to client.
 * </p>
 *
 * @author Todd Wolff todd.wolff_at_prodigy.net
 *
 * @see org.gjt.mm.mysql.jdbc2.Connection
 * @see org.gjt.mm.mysql.jdbc2.optional.MysqlPooledConnection
 */
public class LogicalHandle implements Connection {
    private Connection mc = null;
    private MysqlPooledConnection mpc = null;
    private String invalidHandleStr = "Logical handle no longer valid";
    private boolean closed;

    /**
     * Construct a new LogicalHandle and set instance variables
     *
     * @param mysqlPooledConnection reference to object that instantiated this
     *        object
     * @param mysqlConnection physical connection to db
     *
     * @throws SQLException if an error occurs.
     */
    public LogicalHandle(MysqlPooledConnection mysqlPooledConnection,
        Connection mysqlConnection) throws SQLException {
        mpc = mysqlPooledConnection;
        mc = mysqlConnection;
        closed = false;
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#setAutoCommit
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setAutoCommit(autoCommit);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getAutoCommit();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#setCatalog()
     */
    public void setCatalog(String catalog) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setCatalog(catalog);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @return the current catalog
     *
     * @throws SQLException if an error occurs
     */
    public String getCatalog() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getCatalog();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException {
        return (closed || mc.isClosed());
    }

    /**
     * @see Connection#setHoldability(int)
     */
    public void setHoldability(int arg0) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setHoldability(arg0);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#getHoldability()
     */
    public int getHoldability() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getHoldability();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Allows clients to determine how long this connection has been idle.
     *
     * @return how long the connection has been idle.
     */
    public long getIdleFor() {
        return ((com.mysql.jdbc.Connection) mc).getIdleFor();
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @return a metadata instance
     *
     * @throws SQLException if an error occurs
     */
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getMetaData();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#setReadOnly()
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setReadOnly(readOnly);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.isReadOnly();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#setSavepoint()
     */
    public java.sql.Savepoint setSavepoint() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.setSavepoint();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#setSavepoint(String)
     */
    public java.sql.Savepoint setSavepoint(String arg0)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.setSavepoint(arg0);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#setTransactionIsolation()
     */
    public void setTransactionIsolation(int level) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setTransactionIsolation(level);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getTransactionIsolation();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#setTypeMap()
     */
    public void setTypeMap(java.util.Map map) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.setTypeMap(map);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#getTypeMap()
     */
    public java.util.Map getTypeMap() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getTypeMap();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#getWarnings
     */
    public java.sql.SQLWarning getWarnings() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.getWarnings();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.   Notifies
     * listeners of any caught exceptions before  re-throwing to client.
     *
     * @throws SQLException if an error occurs
     */
    public void clearWarnings() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.clearWarnings();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * The physical connection is not actually closed.  the physical connection
     * is closed when the application server calls
     * mysqlPooledConnection.close().  this object is  de-referenced by the
     * pooled connection each time mysqlPooledConnection.getConnection()  is
     * called by app server.
     *
     * @throws SQLException if an error occurs
     */
    public void close() throws SQLException {
        close(true);
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @throws SQLException if an error occurs
     */
    public void commit() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.commit();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#createStatement()
     */
    public java.sql.Statement createStatement() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.createStatement();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#createStatement()
     */
    public java.sql.Statement createStatement(int resultSetType,
        int resultSetConcurrency) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.createStatement(resultSetType, resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#createStatement(int, int, int)
     */
    public java.sql.Statement createStatement(int arg0, int arg1, int arg2)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.createStatement(arg0, arg1, arg2);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#nativeSQL()
     */
    public String nativeSQL(String sql) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.nativeSQL(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#prepareCall()
     */
    public java.sql.CallableStatement prepareCall(String sql)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareCall(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#prepareCall()
     */
    public java.sql.CallableStatement prepareCall(String sql,
        int resultSetType, int resultSetConcurrency) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareCall(sql, resultSetType, resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#prepareCall(String, int, int, int)
     */
    public java.sql.CallableStatement prepareCall(String arg0, int arg1,
        int arg2, int arg3) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareCall(arg0, arg1, arg2, arg3);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#prepareStatement()
     */
    public java.sql.PreparedStatement prepareStatement(String sql)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#prepareStatement()
     */
    public java.sql.PreparedStatement prepareStatement(String sql,
        int resultSetType, int resultSetConcurrency) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(sql, resultSetType,
                    resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#prepareStatement(String, int, int, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1,
        int arg2, int arg3) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(arg0, arg1, arg2, arg3);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(arg0, arg1);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int[] arg1)
        throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(arg0, arg1);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#prepareStatement(String, String[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0,
        String[] arg1) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                return mc.prepareStatement(arg0, arg1);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#releaseSavepoint(Savepoint)
     */
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.releaseSavepoint(arg0);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * Passes call to method on physical connection instance.  Notifies
     * listeners of any caught exceptions before re-throwing to client.
     *
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.rollback();
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    /**
     * @see Connection#rollback(Savepoint)
     */
    public void rollback(Savepoint arg0) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {
            try {
                mc.rollback(arg0);
            } catch (SQLException sqlException) {
                mpc.callListener(MysqlPooledConnection.CONNECTION_ERROR_EVENT,
                    sqlException);
                throw sqlException;
            }
        }
    }

    protected void close(boolean fireClosedEvent)
        throws SQLException {
    	
    	synchronized (this.mpc) {
    		if (closed) {
    			return;
    		}

    		if (fireClosedEvent) {
    			mpc.callListener(MysqlPooledConnection.CONNECTION_CLOSED_EVENT, null);
    		}

    		// set closed status to true so that if application client tries to make additional
    		// calls a sqlException will be thrown.  The physical connection is
    		// re-used by the pooled connection each time getConnection is called.
    		closed = true;
    	}
    }
}
