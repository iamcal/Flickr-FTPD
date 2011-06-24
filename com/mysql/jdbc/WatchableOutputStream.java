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

import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * A java.io.OutputStream used to write ASCII data into
 * Blobs and Clobs
 *
 * @author Mark Matthews
 */
class WatchableOutputStream extends ByteArrayOutputStream {
    private OutputStreamWatcher watcher;

    /**
     * DOCUMENT ME!
     *
     * @param watcher DOCUMENT ME!
     */
    public void setWatcher(OutputStreamWatcher watcher) {
        this.watcher = watcher;
    }

    /**
     * @see java.io.OutputStream#close()
     */
    public void close() throws IOException {
        super.close();

        if (this.watcher != null) {
            this.watcher.streamClosed(toByteArray());
        }
    }
}
