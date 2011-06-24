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

import java.sql.SQLException;


/**
 * This interface abstracts away how row data is accessed by
 * the result set. It is meant to allow a static implementation
 * (Current version), and a streaming one.
 *
 * @author dgan
 */
public interface RowData {
    /**
     * What's returned for the size of a result set
     * when its size can not be determined.
     */
    public static final int RESULT_SET_SIZE_UNKNOWN = -1;

    /**
     * Returns true if we got the last element.
     *
     * @return true if after last row
     * @throws SQLException if a database error occurs
     */
    boolean isAfterLast() throws SQLException;

    /**
     * Only works on non dynamic result sets.
     *
     * @param index row number to get at
     * @return row data at index
     * @throws SQLException if a database error occurs
     */
    byte[][] getAt(int index) throws SQLException;

    /**
     * Returns if iteration has not occured yet.
     *
     * @return true if before first row
     * @throws SQLException if a database error occurs
     */
    boolean isBeforeFirst() throws SQLException;

    /**
     * Moves the current position in the result set to
     * the given row number.
     *
     * @param rowNumber row to move to
     * @throws SQLException if a database error occurs
     */
    void setCurrentRow(int rowNumber) throws SQLException;

    /**
     * Returns the current position in the result set as
     * a row number.
     *
     * @return the current row number
     * @throws SQLException if a database error occurs
     */
    int getCurrentRowNumber() throws SQLException;

    /**
     * Returns true if the result set is dynamic.
     *
     * This means that move back and move forward won't work
     * because we do not hold on to the records.
     *
     * @return true if this result set is streaming from the server
     * @throws SQLException if a database error occurs
     */
    boolean isDynamic() throws SQLException;

    /**
     * Has no records.
     *
     * @return true if no records
     * @throws SQLException if a database error occurs
     */
    boolean isEmpty() throws SQLException;

    /**
     * Are we on the first row of the result set?
     *
     * @return true if on first row
     * @throws SQLException if a database error occurs
     */
    boolean isFirst() throws SQLException;

    /**
     * Are we on the last row of the result set?
     *
     * @return true if on last row
     * @throws SQLException if a database error occurs
     */
    boolean isLast() throws SQLException;

    /**
     * Adds a row to this row data.
     *
     * @param row the row to add
     * @throws SQLException if a database error occurs
     */
    void addRow(byte[][] row) throws SQLException;

    /**
     * Moves to after last.
     *
     * @throws SQLException if a database error occurs
     */
    void afterLast() throws SQLException;

    /**
     * Moves to before first.
     *
     * @throws SQLException if a database error occurs
     */
    void beforeFirst() throws SQLException;

    /**
     * Moves to before last so next el is the last el.
     *
     * @throws SQLException if a database error occurs
     */
    void beforeLast() throws SQLException;

    /**
     * We're done.
     *
     * @throws SQLException if a database error occurs
     */
    void close() throws SQLException;

    /**
     * Returns true if another row exsists.
     *
     * @return true if more rows
     * @throws SQLException if a database error occurs
     */
    boolean hasNext() throws SQLException;

    /**
     * Moves the current position relative 'rows' from
     * the current position.
     *
     * @param rows the relative number of rows to move
     * @throws SQLException if a database error occurs
     */
    void moveRowRelative(int rows) throws SQLException;

    /**
     * Returns the next row.
     *
     * @return the next row value
     * @throws SQLException if a database error occurs
     */
    byte[][] next() throws SQLException;

    /**
     * Removes the row at the given index.
     *
     * @param index the row to move to
     * @throws SQLException if a database error occurs
     */
    void removeRow(int index) throws SQLException;

    /**
     * Only works on non dynamic result sets.
     *
     * @return the size of this row data
     * @throws SQLException if a database error occurs
     */
    int size() throws SQLException;
    
	/**
	 * Set the result set that 'owns' this RowData
	 * 
	 * @param rs the result set that 'owns' this RowData
	 */
	void setOwner(ResultSet rs);
	
	/**
	 * Returns the result set that 'owns' this RowData
	 * 
	 * @return the result set that 'owns' this RowData
	 */
	ResultSet getOwner();

}
