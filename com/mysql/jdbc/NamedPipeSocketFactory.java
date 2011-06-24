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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import java.net.Socket;
import java.net.SocketException;

import java.util.Properties;


/**
 * A socket factory for named pipes (on Windows)
 *
 * @author Mark Matthews
 */
public class NamedPipeSocketFactory implements SocketFactory {
    private static final String NAMED_PIPE_PROP_NAME = "namedPipePath";
    private Socket namedPipeSocket;

    /**
     * Constructor for NamedPipeSocketFactory.
     */
    public NamedPipeSocketFactory() {
        super();
    }

    /**
     * @see com.mysql.jdbc.SocketFactory#afterHandshake()
     */
    public Socket afterHandshake() throws SocketException, IOException {
        return this.namedPipeSocket;
    }

    /**
     * @see com.mysql.jdbc.SocketFactory#beforeHandshake()
     */
    public Socket beforeHandshake() throws SocketException, IOException {
        return this.namedPipeSocket;
    }

    /**
     * @see com.mysql.jdbc.SocketFactory#connect(String, Properties)
     */
    public Socket connect(String host, Properties props)
        throws SocketException, IOException {
        String namedPipePath = props.getProperty(NAMED_PIPE_PROP_NAME);

        if (namedPipePath == null) {
            namedPipePath = "\\\\.\\pipe\\MySQL";
        } else if (namedPipePath.length() == 0) {
            throw new SocketException(
                "Can not specify NULL or empty value for property '"
                + NAMED_PIPE_PROP_NAME + "'.");
        }

        this.namedPipeSocket = new NamedPipeSocket(namedPipePath);

        return this.namedPipeSocket;
    }

    /**
     * A socket that encapsulates named pipes on Windows
     */
    class NamedPipeSocket extends Socket {
        private RandomAccessFile namedPipeFile;
        private boolean isClosed = false;

        NamedPipeSocket(String filePath) throws IOException {
            if ((filePath == null) || (filePath.length() == 0)) {
                throw new IOException(
                    "Named pipe path can not be null or empty");
            }

            this.namedPipeFile = new RandomAccessFile(filePath, "rw");
        }

        /**
         * @see java.net.Socket#isClosed()
         */
        public boolean isClosed() {
            return this.isClosed;
        }

        /**
         * @see java.net.Socket#getInputStream()
         */
        public InputStream getInputStream() throws IOException {
            return new RandomAccessFileInputStream(this.namedPipeFile);
        }

        /**
         * @see java.net.Socket#getOutputStream()
         */
        public OutputStream getOutputStream() throws IOException {
            return new RandomAccessFileOutputStream(this.namedPipeFile);
        }

        /**
         * @see java.net.Socket#close()
         */
        public synchronized void close() throws IOException {
            this.namedPipeFile.close();
            this.isClosed = true;
        }
    }

    /**
     * Enables OutputStream-type functionality for a RandomAccessFile
     */
    class RandomAccessFileInputStream extends InputStream {
        RandomAccessFile raFile;

        RandomAccessFileInputStream(RandomAccessFile file) {
            this.raFile = file;
        }

        /**
         * @see java.io.InputStream#available()
         */
        public int available() throws IOException {
            return -1;
        }

        /**
         * @see java.io.InputStream#close()
         */
        public void close() throws IOException {
            this.raFile.close();
        }

        /**
         * @see java.io.InputStream#read()
         */
        public int read() throws IOException {
            return this.raFile.read();
        }

        /**
         * @see java.io.InputStream#read(byte[], int, int)
         */
        public int read(byte[] b, int off, int len) throws IOException {
            return this.raFile.read(b, off, len);
        }

        /**
         * @see java.io.InputStream#read(byte[])
         */
        public int read(byte[] b) throws IOException {
            return this.raFile.read(b);
        }
    }

    /**
     * Enables OutputStream-type functionality for a RandomAccessFile
     */
    class RandomAccessFileOutputStream extends OutputStream {
        RandomAccessFile raFile;

        RandomAccessFileOutputStream(RandomAccessFile file) {
            this.raFile = file;
        }

        /**
         * @see java.io.OutputStream#close()
         */
        public void close() throws IOException {
            this.raFile.close();
        }

        /**
         * @see java.io.OutputStream#write(byte[], int, int)
         */
        public void write(byte[] b, int off, int len) throws IOException {
            this.raFile.write(b, off, len);
        }

        /**
         * @see java.io.OutputStream#write(byte[])
         */
        public void write(byte[] b) throws IOException {
            this.raFile.write(b);
        }

        /**
         * @see java.io.OutputStream#write(int)
         */
        public void write(int b) throws IOException {
        }
    }
}
