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
package com.mysql.jdbc;

import java.io.UnsupportedEncodingException;

import java.math.BigDecimal;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * A result set that is updatable.
 *
 * @author Mark Matthews
 */
public class UpdatableResultSet extends ResultSet {
    /** Marker for 'stream' data when doing INSERT rows */
    private final static byte[] STREAM_DATA_MARKER = "** STREAM DATA **"
        .getBytes();

    /** List of primary keys */
    private List primaryKeyIndicies = null;

    /** PreparedStatement used to delete data */
    private com.mysql.jdbc.PreparedStatement deleter = null;

    /** PreparedStatement used to insert data */
    private com.mysql.jdbc.PreparedStatement inserter = null;

    /** PreparedStatement used to refresh data */
    private com.mysql.jdbc.PreparedStatement refresher;

    /** PreparedStatement used to delete data */
    private com.mysql.jdbc.PreparedStatement updater = null;
    private SingleByteCharsetConverter charConverter;
    private String charEncoding;
    private String deleteSQL = null;
    private String insertSQL = null;
    private String quotedIdChar = null;
    private String refreshSQL = null;
    private String tableName;

    /** SQL for in-place modifcation */
    private String updateSQL = null;

    /** What is the default value for the column? */
    private byte[][] defaultColumnValue;

    /** The binary data for the 'current' row */
    private byte[][] savedCurrentRow;

    /** Is this result set updateable? */
    private boolean isUpdatable = false;

    /**
     * Creates a new UpdatableResultSet object.
     *
     * @param updateCount DOCUMENT ME!
     * @param updateID DOCUMENT ME!
     */
    public UpdatableResultSet(long updateCount, long updateID) {
        super(updateCount, updateID);
    }

    // ****************************************************************
    //
    //                       END OF PUBLIC INTERFACE
    //
    // ****************************************************************

    /**
     * Create a new ResultSet - Note that we create ResultSets to represent the
     * results of everything.
     *
     * @param catalog the database in use when this result set was created
     * @param fields an array of Field objects (basically, the ResultSet
     *        MetaData)
     * @param rows Vector of the actual data
     * @param conn the status string returned from the back end
     *
     * @throws SQLException DOCUMENT ME!
     */
    public UpdatableResultSet(String catalog, Field[] fields, RowData rows,
        com.mysql.jdbc.Connection conn) throws SQLException {
        super(catalog, fields, rows, conn);
        isUpdatable = checkUpdatability();
    }

    /**
     * Creates a new UpdatableResultSet object.
     *
     * @param fields DOCUMENT ME!
     * @param rows DOCUMENT ME!
     *
     * @throws SQLException DOCUMENT ME!
     */
    public UpdatableResultSet(Field[] fields, RowData rows)
        throws SQLException {
        super(fields, rows);
        isUpdatable = checkUpdatability();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Determine if the cursor is after the last row in the result  set.
     * </p>
     *
     * @return true if after the last row, false otherwise.  Returns false when
     *         the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isAfterLast() throws SQLException {
        return super.isAfterLast();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Determine if the cursor is before the first row in the result  set.
     * </p>
     *
     * @return true if before the first row, false otherwise. Returns false
     *         when the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isBeforeFirst() throws SQLException {
        return super.isBeforeFirst();
    }

    /**
     * JDBC 2.0 Return the concurrency of this result set.  The concurrency
     * used is determined by the statement that created the result set.
     *
     * @return the concurrency type, CONCUR_READ_ONLY, etc.
     *
     * @exception SQLException if a database-access error occurs
     */
    public int getConcurrency() throws SQLException {
        return (isUpdatable ? CONCUR_UPDATABLE : CONCUR_READ_ONLY);
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Determine if the cursor is on the first row of the result set.
     * </p>
     *
     * @return true if on the first row, false otherwise.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isFirst() throws SQLException {
        return super.isFirst();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Determine if the cursor is on the last row of the result set.    Note:
     * Calling isLast() may be expensive since the JDBC driver might need to
     * fetch ahead one row in order to determine  whether the current row is
     * the last row in the result set.
     * </p>
     *
     * @return true if on the last row, false otherwise.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isLast() throws SQLException {
        return super.isLast();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Move to an absolute row number in the result set.
     * </p>
     * 
     * <p>
     * If row is positive, moves to an absolute row with respect to the
     * beginning of the result set.  The first row is row 1, the second is row
     * 2, etc.
     * </p>
     * 
     * <p>
     * If row is negative, moves to an absolute row position with respect to
     * the end of result set.  For example, calling absolute(-1) positions the
     * cursor on the last row, absolute(-2) indicates the next-to-last row,
     * etc.
     * </p>
     * 
     * <p>
     * An attempt to position the cursor beyond the first/last row in the
     * result set, leaves the cursor before/after the first/last row,
     * respectively.
     * </p>
     * 
     * <p>
     * Note: Calling absolute(1) is the same as calling first(). Calling
     * absolute(-1) is the same as calling last().
     * </p>
     *
     * @param row DOCUMENT ME!
     *
     * @return true if on the result set, false if off.
     *
     * @exception SQLException if a database-access error occurs, or  row is 0,
     *            or result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean absolute(int row) throws SQLException {
        return super.absolute(row);
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves to the end of the result set, just after the last row.  Has no
     * effect if the result set contains no rows.
     * </p>
     *
     * @exception SQLException if a database-access error occurs, or result set
     *            type is TYPE_FORWARD_ONLY.
     */
    public synchronized void afterLast() throws SQLException {
        super.afterLast();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves to the front of the result set, just before the first row. Has no
     * effect if the result set contains no rows.
     * </p>
     *
     * @exception SQLException if a database-access error occurs, or result set
     *            type is TYPE_FORWARD_ONLY
     */
    public synchronized void beforeFirst() throws SQLException {
        super.beforeFirst();
    }

    /**
     * JDBC 2.0 The cancelRowUpdates() method may be called after calling an
     * updateXXX() method(s) and before calling updateRow() to rollback  the
     * updates made to a row.  If no updates have been made or  updateRow()
     * has already been called, then this method has no  effect.
     *
     * @exception SQLException if a database-access error occurs, or if called
     *            when on the insert row.
     */
    public synchronized void cancelRowUpdates() throws SQLException {
        if (doingUpdates) {
            doingUpdates = false;
            updater.clearParameters();
        }
    }

    /**
     * After this call, getWarnings returns null until a new warning is
     * reported for this ResultSet
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized void clearWarnings() throws java.sql.SQLException {
        warningChain = null;
    }

    /**
     * In some cases, it is desirable to immediately release a ResultSet
     * database and JDBC resources instead of waiting for this to happen when
     * it is automatically closed.  The close method provides this immediate
     * release.
     * 
     * <p>
     * <B>Note:</B> A ResultSet is automatically closed by the Statement the
     * Statement that generated it when that Statement is closed, re-executed,
     * or is used to retrieve the next result from a sequence of multiple
     * results.  A ResultSet is also automatically closed when it is garbage
     * collected.
     * </p>
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized void close() throws java.sql.SQLException {
        super.close();
    }

    /**
     * JDBC 2.0 Delete the current row from the result set and the underlying
     * database.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or if called
     *            when on the insert row.
     * @throws SQLException if the ResultSet is not updatable or some other
     *         error occurs
     */
    public synchronized void deleteRow() throws SQLException {
        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (onInsertRow) {
            throw new SQLException(
                "Can not call deleteRow() when on insert row");
        } else if (rowData.size() == 0) {
            throw new SQLException("Can't deleteRow() on empty result set");
        } else if (isBeforeFirst()) {
            throw new SQLException(
                "Before start of result set. Can not call deleteRow().");
        } else if (isAfterLast()) {
            throw new SQLException(
                "After end of result set. Can not call deleteRow().");
        }

        if (deleter == null) {
            if (deleteSQL == null) {
                generateStatements();
            }

            deleter = (com.mysql.jdbc.PreparedStatement) connection
                .prepareStatement(deleteSQL);
            
            if (deleter.getMaxRows() != 0) {
            	deleter.setMaxRows(0);
            }
        }

        deleter.clearParameters();

        String characterEncoding = null;

        if (connection.useUnicode()) {
            characterEncoding = connection.getEncoding();
        }

        //
        // FIXME: Use internal routines where possible for character
        //        conversion!
        try {
            int numKeys = primaryKeyIndicies.size();

            if (numKeys == 1) {
                int index = ((Integer) primaryKeyIndicies.get(0)).intValue();
                String currentVal = ((characterEncoding == null)
                    ? new String(thisRow[index])
                    : new String(thisRow[index], characterEncoding));
                deleter.setString(1, currentVal);
            } else {
                for (int i = 0; i < numKeys; i++) {
                    int index = ((Integer) primaryKeyIndicies.get(i)).intValue();
                    String currentVal = ((characterEncoding == null)
                        ? new String(thisRow[index])
                        : new String(thisRow[index], characterEncoding));
                    deleter.setString(i + 1, currentVal);
                }
            }

            deleter.executeUpdate();
            rowData.removeRow(rowData.getCurrentRowNumber());
        } catch (java.io.UnsupportedEncodingException encodingEx) {
            throw new SQLException("Unsupported character encoding '"
                + connection.getEncoding() + "'");
        }
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves to the first row in the result set.
     * </p>
     *
     * @return true if on a valid row, false if no rows in the result set.
     *
     * @exception SQLException if a database-access error occurs, or result set
     *            type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean first() throws SQLException {
        return super.first();
    }

    /**
     * JDBC 2.0 Insert the contents of the insert row into the result set and
     * the database.  Must be on the insert row when this method is called.
     *
     * @exception SQLException if a database-access error occurs, if called
     *            when not on the insert row, or if all non-nullable columns
     *            in the insert row have not been given a value
     */
    public synchronized void insertRow() throws SQLException {
        if (!onInsertRow) {
            throw new SQLException("Not on insert row");
        } else {
            inserter.executeUpdate();

            int numPrimaryKeys = 0;

            if (primaryKeyIndicies != null) {
                numPrimaryKeys = primaryKeyIndicies.size();
            }

            long autoIncrementId = inserter.getLastInsertID();
            int numFields = fields.length;
            byte[][] newRow = new byte[numFields][];

            for (int i = 0; i < numFields; i++) {
                if (inserter.isNull(i)) {
                    newRow[i] = null;
                } else {
                    newRow[i] = inserter.getBytes(i);
                }

                if ((numPrimaryKeys == 1) && fields[i].isPrimaryKey()
                        && (autoIncrementId > 0)) {
                    newRow[i] = String.valueOf(autoIncrementId).getBytes();
                }
            }

            rowData.addRow(newRow);
            resetInserter();
        }
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves to the last row in the result set.
     * </p>
     *
     * @return true if on a valid row, false if no rows in the result set.
     *
     * @exception SQLException if a database-access error occurs, or result set
     *            type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean last() throws SQLException {
        return super.last();
    }

    /**
     * JDBC 2.0 Move the cursor to the remembered cursor position, usually the
     * current row.  Has no effect unless the cursor is on the insert  row.
     *
     * @exception SQLException if a database-access error occurs, or the result
     *            set is not updatable
     * @throws SQLException if the ResultSet is not updatable or some other
     *         error occurs
     */
    public synchronized void moveToCurrentRow() throws SQLException {
        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (this.onInsertRow) {
            onInsertRow = false;
            this.thisRow = this.savedCurrentRow;
        }
    }

    /**
     * JDBC 2.0 Move to the insert row.  The current cursor position is
     * remembered while the cursor is positioned on the insert row. The insert
     * row is a special row associated with an updatable result set.  It is
     * essentially a buffer where a new row may be constructed by calling the
     * updateXXX() methods prior to  inserting the row into the result set.
     * Only the updateXXX(), getXXX(), and insertRow() methods may be  called
     * when the cursor is on the insert row.  All of the columns in  a result
     * set must be given a value each time this method is called before
     * calling insertRow().  UpdateXXX()must be called before getXXX() on a
     * column.
     *
     * @exception SQLException if a database-access error occurs, or the result
     *            set is not updatable
     * @throws NotUpdatable DOCUMENT ME!
     */
    public synchronized void moveToInsertRow() throws SQLException {
        if (!this.isUpdatable) {
            throw new NotUpdatable();
        }

        if (this.inserter == null) {
            generateStatements();
            this.inserter = (com.mysql.jdbc.PreparedStatement) connection
                .prepareStatement(this.insertSQL);
            
            if (this.inserter.getMaxRows() != 0) {
            	this.inserter.setMaxRows(0);
            }
            
            extractDefaultValues();
            resetInserter();
        } else {
            resetInserter();
        }

		int numFields = this.fields.length;
		
		this.onInsertRow = true;
		this.doingUpdates = false;
		this.savedCurrentRow = this.thisRow;
		this.thisRow = new byte[numFields][];
			   
        for (int i = 0; i < numFields; i++) {
            if (this.defaultColumnValue[i] != null) {
                this.inserter.setBytes(i + 1, this.defaultColumnValue[i]);
                
                // This value _could_ be changed from a getBytes(), so we
                // need a copy....
                byte[] defaultValueCopy = new byte[this.defaultColumnValue[i].length];
                System.arraycopy(defaultColumnValue[i], 0, defaultValueCopy, 0, defaultValueCopy.length);
                this.thisRow[i] = defaultValueCopy;
            } else {
                this.inserter.setNull(i + 1, java.sql.Types.NULL);
                this.thisRow[i] = null;
            }
        }
    }

    /**
     * A ResultSet is initially positioned before its first row, the first call
     * to next makes the first row the current row; the second call makes the
     * second row the current row, etc.
     * 
     * <p>
     * If an input stream from the previous row is open, it is implicitly
     * closed.  The ResultSet's warning chain is cleared when a new row is
     * read
     * </p>
     *
     * @return true if the new current is valid; false if there are no more
     *         rows
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized boolean next() throws java.sql.SQLException {
        return super.next();
    }

    /**
     * The prev method is not part of JDBC, but because of the architecture of
     * this driver it is possible to move both forward and backward within the
     * result set.
     * 
     * <p>
     * If an input stream from the previous row is open, it is implicitly
     * closed.  The ResultSet's warning chain is cleared when a new row is
     * read
     * </p>
     *
     * @return true if the new current is valid; false if there are no more
     *         rows
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized boolean prev() throws java.sql.SQLException {
        return super.prev();
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves to the previous row in the result set.
     * </p>
     * 
     * <p>
     * Note: previous() is not the same as relative(-1) since it makes sense to
     * call previous() when there is no current row.
     * </p>
     *
     * @return true if on a valid row, false if off the result set.
     *
     * @exception SQLException if a database-access error occurs, or result set
     *            type is TYPE_FORWAR_DONLY.
     */
    public synchronized boolean previous() throws SQLException {
        return super.previous();
    }

    /**
     * JDBC 2.0 Refresh the value of the current row with its current value in
     * the database.  Cannot be called when on the insert row. The
     * refreshRow() method provides a way for an application to  explicitly
     * tell the JDBC driver to refetch a row(s) from the database.  An
     * application may want to call refreshRow() when  caching or prefetching
     * is being done by the JDBC driver to fetch the latest value of a row
     * from the database.  The JDBC driver  may actually refresh multiple rows
     * at once if the fetch size is  greater than one.  All values are
     * refetched subject to the transaction isolation  level and cursor
     * sensitivity.  If refreshRow() is called after calling updateXXX(), but
     * before calling updateRow() then the updates made to the row are lost.
     * Calling refreshRow() frequently will likely slow performance.
     *
     * @exception SQLException if a database-access error occurs, or if called
     *            when on the insert row.
     * @throws NotUpdatable DOCUMENT ME!
     */
    public synchronized void refreshRow() throws SQLException {
        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (onInsertRow) {
            throw new SQLException(
                "Can not call refreshRow() when on insert row");
        } else if (rowData.size() == 0) {
            throw new SQLException("Can't refreshRow() on empty result set");
        } else if (isBeforeFirst()) {
            throw new SQLException(
                "Before start of result set. Can not call refreshRow().");
        } else if (isAfterLast()) {
            throw new SQLException(
                "After end of result set. Can not call refreshRow().");
        }

        if (refresher == null) {
            if (refreshSQL == null) {
                generateStatements();
            }

            refresher = (com.mysql.jdbc.PreparedStatement) connection
                .prepareStatement(refreshSQL);
            
            if (refresher.getMaxRows() != 0) {
            	refresher.setMaxRows(0);
            }
        }

        refresher.clearParameters();

        int numKeys = primaryKeyIndicies.size();

        if (numKeys == 1) {
            byte[] dataFrom = null;
            int index = ((Integer) primaryKeyIndicies.get(0)).intValue();

            if (!doingUpdates) {
                dataFrom = thisRow[index];
            } else {
                dataFrom = updater.getBytes(index);

                // Primary keys not set?
                if (updater.isNull(index) || (dataFrom.length == 0)) {
                    dataFrom = thisRow[index];
                }
            }

            refresher.setBytesNoEscape(1, dataFrom);
        } else {
            for (int i = 0; i < numKeys; i++) {
                byte[] dataFrom = null;
                int index = ((Integer) primaryKeyIndicies.get(i)).intValue();

                if (!doingUpdates) {
                    dataFrom = thisRow[index];
                } else {
                    dataFrom = updater.getBytes(index);

                    // Primary keys not set?
                    if (updater.isNull(index) || (dataFrom.length == 0)) {
                        dataFrom = thisRow[index];
                    }
                }

                refresher.setBytesNoEscape(i + 1, dataFrom);
            }
        }

        java.sql.ResultSet rs = null;

        try {
            rs = refresher.executeQuery();

            int numCols = rs.getMetaData().getColumnCount();

            if (rs.next()) {
                for (int i = 0; i < numCols; i++) {
                    byte[] val = rs.getBytes(i + 1);

                    if ((val == null) || rs.wasNull()) {
                        thisRow[i] = null;
                    } else {
                        thisRow[i] = rs.getBytes(i + 1);
                    }
                }
            } else {
                throw new SQLException("refreshRow() called on row that has been deleted or had primary key changed",
                    SQLError.SQL_STATE_GENERAL_ERROR);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    ; // ignore
                }
            }
        }
    }

    /**
     * JDBC 2.0
     * 
     * <p>
     * Moves a relative number of rows, either positive or negative. Attempting
     * to move beyond the first/last row in the result set positions the
     * cursor before/after the the first/last row. Calling relative(0) is
     * valid, but does not change the cursor position.
     * </p>
     * 
     * <p>
     * Note: Calling relative(1) is different than calling next() since is
     * makes sense to call next() when there is no current row, for example,
     * when the cursor is positioned before the first row or after the last
     * row of the result set.
     * </p>
     *
     * @param rows DOCUMENT ME!
     *
     * @return true if on a row, false otherwise.
     *
     * @exception SQLException if a database-access error occurs, or there is
     *            no current row, or result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean relative(int rows) throws SQLException {
        return super.relative(rows);
    }

    /**
     * JDBC 2.0 Determine if this row has been deleted.  A deleted row may
     * leave a visible "hole" in a result set.  This method can be used to
     * detect holes in a result set.  The value returned depends on whether or
     * not the result set can detect deletions.
     *
     * @return true if deleted and deletes are detected
     *
     * @exception SQLException if a database-access error occurs
     * @throws NotImplemented DOCUMENT ME!
     *
     * @see DatabaseMetaData#deletesAreDetected
     */
    public synchronized boolean rowDeleted() throws SQLException {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0 Determine if the current row has been inserted.  The value
     * returned  depends on whether or not the result set can detect visible
     * inserts.
     *
     * @return true if inserted and inserts are detected
     *
     * @exception SQLException if a database-access error occurs
     * @throws NotImplemented DOCUMENT ME!
     *
     * @see DatabaseMetaData#insertsAreDetected
     */
    public synchronized boolean rowInserted() throws SQLException {
        throw new NotImplemented();
    }

    //---------------------------------------------------------------------
    // Updates
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0 Determine if the current row has been updated.  The value
     * returned  depends on whether or not the result set can detect updates.
     *
     * @return true if the row has been visibly updated by the owner or
     *         another, and updates are detected
     *
     * @exception SQLException if a database-access error occurs
     * @throws NotImplemented DOCUMENT ME!
     *
     * @see DatabaseMetaData#updatesAreDetected
     */
    public synchronized boolean rowUpdated() throws SQLException {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0  Update a column with an ascii stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateAsciiStream(int columnIndex,
        java.io.InputStream x, int length) throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setAsciiStream(columnIndex, x, length);
        } else {
            inserter.setAsciiStream(columnIndex, x, length);
            this.thisRow[columnIndex - 1] = STREAM_DATA_MARKER;
        }
    }

    /**
     * JDBC 2.0  Update a column with an ascii stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateAsciiStream(String columnName,
        java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    /**
     * JDBC 2.0  Update a column with a BigDecimal value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBigDecimal(int columnIndex, BigDecimal x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBigDecimal(columnIndex, x);
        } else {
            inserter.setBigDecimal(columnIndex, x);

            if (x == null) {
                this.thisRow[columnIndex - 1] = null;
            } else {
                this.thisRow[columnIndex - 1] = x.toString().getBytes();
            }
        }
    }

    /**
     * JDBC 2.0  Update a column with a BigDecimal value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBigDecimal(String columnName, BigDecimal x)
        throws SQLException {
        updateBigDecimal(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a binary stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBinaryStream(int columnIndex,
        java.io.InputStream x, int length) throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBinaryStream(columnIndex, x, length);
        } else {
            inserter.setBinaryStream(columnIndex, x, length);

            if (x == null) {
                this.thisRow[columnIndex - 1] = null;
            } else {
                this.thisRow[columnIndex - 1] = STREAM_DATA_MARKER;
            }
        }
    }

    /**
     * JDBC 2.0  Update a column with a binary stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBinaryStream(String columnName,
        java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    /**
     * @see ResultSet#updateBlob(int, Blob)
     */
    public void updateBlob(int columnIndex, java.sql.Blob blob)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBlob(columnIndex, blob);
        } else {
            inserter.setBlob(columnIndex, blob);

            if (blob == null) {
                this.thisRow[columnIndex - 1] = null;
            } else {
                this.thisRow[columnIndex - 1] = STREAM_DATA_MARKER;
            }
        }
    }

    /**
     * @see ResultSet#updateBlob(String, Blob)
     */
    public void updateBlob(String columnName, java.sql.Blob blob)
        throws SQLException {
        updateBlob(findColumn(columnName), blob);
    }

    /**
     * JDBC 2.0  Update a column with a boolean value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBoolean(int columnIndex, boolean x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBoolean(columnIndex, x);
        } else {
            inserter.setBoolean(columnIndex, x);

            this.thisRow[columnIndex - 1] = inserter.getBytes(1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a boolean value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBoolean(String columnName, boolean x)
        throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a byte value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateByte(int columnIndex, byte x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setByte(columnIndex, x);
        } else {
            inserter.setByte(columnIndex, x);

            this.thisRow[columnIndex - 1] = inserter.getBytes(columnIndex);
        }
    }

    /**
     * JDBC 2.0  Update a column with a byte value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateByte(String columnName, byte x)
        throws SQLException {
        updateByte(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a byte array value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBytes(int columnIndex, byte[] x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBytes(columnIndex, x);
        } else {
            inserter.setBytes(columnIndex, x);

            this.thisRow[columnIndex - 1] = x;
        }
    }

    /**
     * JDBC 2.0  Update a column with a byte array value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBytes(String columnName, byte[] x)
        throws SQLException {
        updateBytes(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a character stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateCharacterStream(int columnIndex,
        java.io.Reader x, int length) throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setCharacterStream(columnIndex, x, length);
        } else {
            inserter.setCharacterStream(columnIndex, x, length);

            if (x == null) {
                this.thisRow[columnIndex - 1] = null;
            } else {
                this.thisRow[columnIndex - 1] = STREAM_DATA_MARKER;
            }
        }
    }

    /**
     * JDBC 2.0  Update a column with a character stream value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param reader the new column value
     * @param length of the stream
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateCharacterStream(String columnName,
        java.io.Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnName), reader, length);
    }
    
	/**
	  * @see ResultSet#updateClob(int, Clob)
	  */
	 public void updateClob(int columnIndex, java.sql.Clob clob) throws SQLException {
	 	 if (clob == null) {
	 	 	updateNull(columnIndex);
	 	 } else {
	 	 	updateCharacterStream(columnIndex, clob.getCharacterStream(), (int) clob.length());
	 	 }
	 }

    /**
     * JDBC 2.0  Update a column with a Date value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDate(int columnIndex, java.sql.Date x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setDate(columnIndex, x);
        } else {
            inserter.setDate(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a Date value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDate(String columnName, java.sql.Date x)
        throws SQLException {
        updateDate(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a Double value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDouble(int columnIndex, double x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setDouble(columnIndex, x);
        } else {
            inserter.setDouble(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a double value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDouble(String columnName, double x)
        throws SQLException {
        updateDouble(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a float value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateFloat(int columnIndex, float x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setFloat(columnIndex, x);
        } else {
            inserter.setFloat(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a float value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateFloat(String columnName, float x)
        throws SQLException {
        updateFloat(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with an integer value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateInt(int columnIndex, int x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setInt(columnIndex, x);
        } else {
            inserter.setInt(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with an integer value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateInt(String columnName, int x)
        throws SQLException {
        updateInt(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a long value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateLong(int columnIndex, long x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setLong(columnIndex, x);
        } else {
            inserter.setLong(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a long value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateLong(String columnName, long x)
        throws SQLException {
        updateLong(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Give a nullable column a null value.  The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateNull(int columnIndex)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setNull(columnIndex, 0);
        } else {
            inserter.setNull(columnIndex, 0);

            this.thisRow[columnIndex - 1] = null;
        }
    }

    /**
     * JDBC 2.0  Update a column with a null value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnName the name of the column
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateNull(String columnName)
        throws SQLException {
        updateNull(findColumn(columnName));
    }

    /**
     * JDBC 2.0  Update a column with an Object value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *        this is the number of digits after the decimal.  For all other
     *        types this value will be ignored.
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(int columnIndex, Object x, int scale)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setObject(columnIndex, x);
        } else {
            inserter.setObject(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with an Object value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(int columnIndex, Object x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setObject(columnIndex, x);
        } else {
            inserter.setObject(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with an Object value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *        this is the number of digits after the decimal.  For all other
     *        types this value will be ignored.
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(String columnName, Object x, int scale)
        throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with an Object value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(String columnName, Object x)
        throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0 Update the underlying database with the new contents of the
     * current row.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or if called
     *            when on the insert row
     * @throws NotUpdatable DOCUMENT ME!
     */
    public synchronized void updateRow() throws SQLException {
        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (doingUpdates) {
            updater.executeUpdate();
            refreshRow();
            doingUpdates = false;
        }

        //
        // fixes calling updateRow() and then doing more
        // updates on same row...
        syncUpdate();
    }

    /**
     * JDBC 2.0  Update a column with a short value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateShort(int columnIndex, short x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setShort(columnIndex, x);
        } else {
            inserter.setShort(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a short value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateShort(String columnName, short x)
        throws SQLException {
        updateShort(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a String value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateString(int columnIndex, String x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setString(columnIndex, x);
        } else {
            inserter.setString(columnIndex, x);

            if (x == null) {
                this.thisRow[columnIndex - 1] = null;
            } else {
                if (getCharConverter() != null) {
                    try {
                        this.thisRow[columnIndex - 1] = StringUtils.getBytes(x,
                                this.charConverter, this.charEncoding);
                    } catch (UnsupportedEncodingException uEE) {
                        throw new SQLException(
                            "Unsupported character encoding '"
                            + this.charEncoding + "'", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
                    }
                } else {
                    this.thisRow[columnIndex - 1] = x.getBytes();
                }
            }
        }
    }

    /**
     * JDBC 2.0  Update a column with a String value. The updateXXX() methods
     * are used to update column values in the current row, or the insert row.
     * The updateXXX() methods do not  update the underlying database, instead
     * the updateRow() or insertRow() methods are called to update the
     * database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateString(String columnName, String x)
        throws SQLException {
        updateString(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a Time value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTime(int columnIndex, java.sql.Time x)
        throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setTime(columnIndex, x);
        } else {
            inserter.setTime(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a Time value. The updateXXX() methods are
     * used to update column values in the current row, or the insert row. The
     * updateXXX() methods do not  update the underlying database, instead the
     * updateRow() or insertRow() methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTime(String columnName, java.sql.Time x)
        throws SQLException {
        updateTime(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0  Update a column with a Timestamp value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTimestamp(int columnIndex,
        java.sql.Timestamp x) throws SQLException {
        if (!onInsertRow) {
            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setTimestamp(columnIndex, x);
        } else {
            inserter.setTimestamp(columnIndex, x);

            this.thisRow[columnIndex - 1] = this.inserter.getBytes(columnIndex
                    - 1);
        }
    }

    /**
     * JDBC 2.0  Update a column with a Timestamp value. The updateXXX()
     * methods are used to update column values in the current row, or the
     * insert row.  The updateXXX() methods do not  update the underlying
     * database, instead the updateRow() or insertRow() methods are called to
     * update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     *
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTimestamp(String columnName,
        java.sql.Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnName), x);
    }

    /**
     * Sets the concurrency type of this result set
     *
     * @param concurrencyFlag the type of concurrency that this ResultSet
     *        should support.
     */
    protected void setResultSetConcurrency(int concurrencyFlag) {
        super.setResultSetConcurrency(concurrencyFlag);

        //
        // FIXME: Issue warning when asked for updateable result set, but result set is not
        // updatable
        //
        //if ((concurrencyFlag == CONCUR_UPDATABLE) && !isUpdatable()) {
        //java.sql.SQLWarning warning = new java.sql.SQLWarning(
        //NotUpdatable.NOT_UPDATEABLE_MESSAGE);
        //}
    }

    protected void checkRowPos() throws SQLException {
        // don't use RowData's idea of
        // row bounds when we're doing
        // inserts...
        if (!this.onInsertRow) {
            super.checkRowPos();
        }
    }

    /**
     * Figure out whether or not this ResultSet is updateable, and if so,
     * generate the PreparedStatements to support updates.
     *
     * @throws SQLException DOCUMENT ME!
     * @throws NotUpdatable DOCUMENT ME!
     */
    protected void generateStatements() throws SQLException {
        if (!isUpdatable) {
        	this.doingUpdates = false;
        	this.onInsertRow = false;
        	
            throw new NotUpdatable();
        }

        String quotedId = getQuotedIdChar();

        this.tableName = null;

        if (fields[0].getOriginalTableName() != null) {
            StringBuffer tableNameBuffer = new StringBuffer();

            String databaseName = fields[0].getDatabaseName();

            if ((databaseName != null) && (databaseName.length() > 0)) {
                tableNameBuffer.append(quotedId);
                tableNameBuffer.append(databaseName);
                tableNameBuffer.append(quotedId);
                tableNameBuffer.append('.');
            }

            tableNameBuffer.append(quotedId);
            tableNameBuffer.append(fields[0].getOriginalTableName());
            tableNameBuffer.append(quotedId);

            this.tableName = tableNameBuffer.toString();
        } else {
            StringBuffer tableNameBuffer = new StringBuffer();

            tableNameBuffer.append(quotedId);
            tableNameBuffer.append(fields[0].getTableName());
            tableNameBuffer.append(quotedId);

            this.tableName = tableNameBuffer.toString();
        }

        primaryKeyIndicies = new ArrayList();

        StringBuffer fieldValues = new StringBuffer();
        StringBuffer keyValues = new StringBuffer();
        StringBuffer columnNames = new StringBuffer();
        StringBuffer insertPlaceHolders = new StringBuffer();
        boolean firstTime = true;
        boolean keysFirstTime = true;

        for (int i = 0; i < fields.length; i++) {
            String originalColumnName = fields[i].getOriginalName();
            String columnName = null;

            if (this.connection.getIO().hasLongColumnInfo()
                    && (originalColumnName != null)
                    && (originalColumnName.length() > 0)) {
                columnName = originalColumnName;
            } else {
                columnName = fields[i].getName();
            }

            if (fields[i].isPrimaryKey()) {
                primaryKeyIndicies.add(new Integer(i));

                if (!keysFirstTime) {
                    keyValues.append(" AND ");
                } else {
                    keysFirstTime = false;
                }

                keyValues.append(quotedId);
                keyValues.append(columnName);
                keyValues.append(quotedId);
                keyValues.append("=?");
            }

            if (firstTime) {
                firstTime = false;
                fieldValues.append("SET ");
            } else {
                fieldValues.append(",");
                columnNames.append(",");
                insertPlaceHolders.append(",");
            }

            insertPlaceHolders.append("?");

            columnNames.append(quotedId);
            columnNames.append(columnName);
            columnNames.append(quotedId);

            fieldValues.append(quotedId);
            fieldValues.append(columnName);
            fieldValues.append(quotedId);
            fieldValues.append("=?");
        }

        updateSQL = "UPDATE " + this.tableName + " " + fieldValues.toString()
            + " WHERE " + keyValues.toString();
        insertSQL = "INSERT INTO " + this.tableName + " ("
            + columnNames.toString() + ") VALUES ("
            + insertPlaceHolders.toString() + ")";
        refreshSQL = "SELECT " + columnNames.toString() + " FROM " + tableName
            + " WHERE " + keyValues.toString();
        deleteSQL = "DELETE FROM " + this.tableName + " WHERE "
            + keyValues.toString();
    }

    boolean isUpdatable() {
        return this.isUpdatable;
    }

    /**
     * Reset UPDATE prepared statement to value in current row.  This_Row MUST
     * point to current, valid row.
     *
     * @throws SQLException DOCUMENT ME!
     */
    void syncUpdate() throws SQLException {
        if (updater == null) {
            if (updateSQL == null) {
                generateStatements();
            }

            updater = (com.mysql.jdbc.PreparedStatement) connection
                .prepareStatement(updateSQL);
            
            if (updater.getMaxRows() != 0) {
            	updater.setMaxRows(0);
            }
        }

        int numFields = fields.length;
        updater.clearParameters();

        for (int i = 0; i < numFields; i++) {
            if (thisRow[i] != null) {
                updater.setBytes(i + 1, thisRow[i]);
            } else {
                updater.setNull(i + 1, 0);
            }
        }

        int numKeys = primaryKeyIndicies.size();

        if (numKeys == 1) {
            int index = ((Integer) primaryKeyIndicies.get(0)).intValue();
            byte[] keyData = thisRow[index];
            updater.setBytes(numFields + 1, keyData);
        } else {
            for (int i = 0; i < numKeys; i++) {
                byte[] currentVal = thisRow[((Integer) primaryKeyIndicies.get(i))
                    .intValue()];

                if (currentVal != null) {
                    updater.setBytes(numFields + i + 1, currentVal);
                } else {
                    updater.setNull(numFields + i + 1, 0);
                }
            }
        }
    }

    private boolean initializedCharConverter = false;
    
    private synchronized SingleByteCharsetConverter getCharConverter()
        throws SQLException {
        if (!this.initializedCharConverter) {
        	this.initializedCharConverter = true;
        	
            if (this.connection.useUnicode()) {
                this.charEncoding = connection.getEncoding();
                this.charConverter = this.connection.getCharsetConverter(this.charEncoding);
            }
        }

        return this.charConverter;
    }

    private synchronized String getQuotedIdChar() throws SQLException {
        if (this.quotedIdChar == null) {
            boolean useQuotedIdentifiers = connection.supportsQuotedIdentifiers();

            if (useQuotedIdentifiers) {
                java.sql.DatabaseMetaData dbmd = connection.getMetaData();
                this.quotedIdChar = dbmd.getIdentifierQuoteString();
            } else {
                this.quotedIdChar = "";
            }
        }

        return this.quotedIdChar;
    }

    /**
     * DOCUMENT ME!
     *
     * @param field
     *
     * @return String
     */
    private String getTableName(Field field) {
        String originalTableName = field.getOriginalTableName();

        if ((originalTableName != null) && (originalTableName.length() > 0)) {
            return originalTableName;
        } else {
            return field.getTableName();
        }
    }

    /**
     * Is this ResultSet updateable?
     *
     * @return DOCUMENT ME!
     *
     * @throws SQLException DOCUMENT ME!
     */
	private boolean checkUpdatability() throws SQLException {
		String tableName = null;
		String catalogName = null;

		int primaryKeyCount = 0;

		if (fields.length > 0) {
			
			tableName = fields[0].getOriginalTableName();
            catalogName = fields[0].getDatabaseName();
            
			if (tableName == null) {
				tableName = fields[0].getTableName();
				catalogName = this.catalog;
			}

			if (fields[0].isPrimaryKey()) {
				primaryKeyCount++;
			}

			//
			// References only one table?
			//
			for (int i = 1; i < fields.length; i++) {
				String otherTableName = fields[i].getOriginalTableName();
            	
				String otherCatalogName = fields[i].getDatabaseName();
				
				if (otherTableName == null) {
					otherTableName = fields[i].getTableName();
					otherCatalogName = this.catalog;
				}
            	
				// Can't have more than one table
				if ((tableName == null)
						|| !otherTableName.equals(tableName)) {
					return false;
				}

				// Can't reference more than one database
				if ((catalogName == null) || !otherCatalogName.equals(catalogName)) {
					return false;
				}
				
				if (fields[i].isPrimaryKey()) {
					primaryKeyCount++;
				}
			}

			if ((tableName == null) || (tableName.length() == 0)) {
				return false;
			}
		} else {
			return false;
		}

		// 
		// Must have at least one primary key
		//
		if (primaryKeyCount == 0) {
			return false;
		}

		// We can only do this if we know that there is a currently
		// selected database, or if we're talking to a > 4.1 version
		// of MySQL server (as it returns database names in field
		// info)
		//
		if ((this.catalog == null) || (this.catalog.length() == 0)) {
			this.catalog = fields[0].getDatabaseName();

			if ((this.catalog == null) || (this.catalog.length() == 0)) {
				throw new SQLException(
				"Can not create updatable result sets when there is no currently selected database"
				+ " and MySQL server version < 4.1", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
			}
		}

		if (this.connection.useStrictUpdates()) {
			java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();

			java.sql.ResultSet rs = null;
			HashMap primaryKeyNames = new HashMap();

			try {
				rs = dbmd.getPrimaryKeys(catalogName, null, tableName);

				while (rs.next()) {
					String keyName = rs.getString(4);
					keyName = keyName.toUpperCase();
					primaryKeyNames.put(keyName, keyName);
				}
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception ex) {
						AssertionFailedException.shouldNotHappen(ex);
					}

					rs = null;
				}
			}

			if (primaryKeyNames.size() == 0) {
				return false; // we can't update tables w/o keys
			}

			//
			// Contains all primary keys?
			//
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].isPrimaryKey()) {
					String columnNameUC = fields[i].getName().toUpperCase();

					if (primaryKeyNames.remove(columnNameUC) == null) {
						// try original name
						String originalName = fields[i].getOriginalName();

						if (originalName != null) {
							if (primaryKeyNames.remove(
										originalName.toUpperCase()) == null) {
								// we don't know about this key, so give up :(
								return false;
							}
						}
					}
				}
			}

			return primaryKeyNames.isEmpty();
		}

		return true;
	}

    private synchronized void extractDefaultValues() throws SQLException {
        java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();

        java.sql.ResultSet columnsResultSet = null;

        try {
            String unquotedTableName = this.tableName;

            if (unquotedTableName.startsWith(this.quotedIdChar)) {
                unquotedTableName = unquotedTableName.substring(1);
            }

            if (unquotedTableName.endsWith(this.quotedIdChar)) {
                unquotedTableName = unquotedTableName.substring(0,
                        unquotedTableName.length() - 1);
            }

            columnsResultSet = dbmd.getColumns(this.catalog, null,
                    unquotedTableName, "%");

            HashMap columnNameToDefaultValueMap = new HashMap(this.fields.length /* at least this big... */);

            while (columnsResultSet.next()) {
                String columnName = columnsResultSet.getString("COLUMN_NAME");
                byte[] defaultValue = columnsResultSet.getBytes("COLUMN_DEF");

                columnNameToDefaultValueMap.put(columnName, defaultValue);
            }

            int numFields = this.fields.length;

            this.defaultColumnValue = new byte[numFields][];

            for (int i = 0; i < numFields; i++) {
                String tableName = this.fields[i].getOriginalName();

                if ((tableName == null) || (tableName.length() == 0)) {
                    tableName = this.fields[i].getName();
                }

                if (tableName != null) {
                    byte[] defaultVal = (byte[]) columnNameToDefaultValueMap
                        .get(tableName);

                    this.defaultColumnValue[i] = defaultVal;
                }
            }
        } finally {
            if (columnsResultSet != null) {
                columnsResultSet.close();

                columnsResultSet = null;
            }
        }
    }

    private void resetInserter() throws SQLException {
        inserter.clearParameters();

        for (int i = 0; i < fields.length; i++) {
            inserter.setNull(i + 1, 0);
        }
    }
}
