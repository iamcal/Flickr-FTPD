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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


/**
 * @author Mark Matthews
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
class CompressedInputStream extends InputStream {
    //~ Instance fields --------------------------------------------------------

    /**
     * The ZIP inflater used to un-compress packets
     */
    private Inflater inflater;

    /**
     * The stream we are reading from the server
     */
    private InputStream in;

    /**
     * The packet data after it has been un-compressed
     */
    private byte[] uncompressedPacket;

    /**
     * The position we are reading from
     */
    private int pos = 0;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CompressedInputStream that reads
     * the given stream from the server.
     *
     * @param streamFromServer
     */
    public CompressedInputStream(InputStream streamFromServer) {
        this.in = streamFromServer;
        this.inflater = new Inflater();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @see java.io.InputStream#available()
     */
    public int available() throws IOException {
        if (this.uncompressedPacket == null) {
            return this.in.available();
        }

        return this.uncompressedPacket.length - this.pos + this.in.available();
    }

    /**
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        this.in.close();
        this.uncompressedPacket = null;
        this.inflater = null;
    }

    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException {
        try {
            getNextPacketIfRequired(1);
        } catch (IOException ioEx) {
            return -1;
        }

        return this.uncompressedPacket[this.pos++] & 0xff;
    }

    /**
     * @see java.io.InputStream#read(byte, int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }

        if (len <= 0) {
            return 0;
        }

        try {
            getNextPacketIfRequired(len);
        } catch (IOException ioEx) {
            return -1;
        }

        System.arraycopy(this.uncompressedPacket, this.pos, b, off, len);
        this.pos += len;

        return len;
    }

    /**
     * @see java.io.InputStream#read(byte)
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * @see java.io.InputStream#skip(long)
     */
    public long skip(long n) throws IOException {
        long count = 0;

        for (long i = 0; i < n; i++) {
            int bytesRead = read();

            if (bytesRead == -1) {
                break;
            }

            count++;
        }

        return count;
    }

    /**
         * Retrieves and un-compressed (if necessary) the next
         * packet from the server.
         *
         * @throws IOException if an I/O error occurs
         */
    private void getNextPacketFromServer() throws IOException {
        byte[] uncompressedBuffer = null;

        int packetLength = this.in.read() + (this.in.read() << 8)
            + (this.in.read() << 16);

        // -1 for all values through above assembly sequence
        if (packetLength == -65793) {
            throw new IOException("Unexpected end of input stream");
        }

        // we don't look at packet sequence in this case
        this.in.read();

        int compressedLength = this.in.read() + (this.in.read() << 8)
            + (this.in.read() << 16);

        if (compressedLength > 0) {
            uncompressedBuffer = new byte[compressedLength];

            byte[] compressedBuffer = new byte[packetLength];

            readFully(compressedBuffer, 0, packetLength);

            try {
                this.inflater.reset();
            } catch (NullPointerException npe) {
                this.inflater = new Inflater();
            }

            this.inflater.setInput(compressedBuffer);

            try {
                this.inflater.inflate(uncompressedBuffer);
            } catch (DataFormatException dfe) {
                throw new IOException(
                    "Error while uncompressing packet from server.");
            }

            this.inflater.end();
        } else {
            //	
            //	Read data, note this this code is reached when using
            //  compressed packets that have not been compressed, as well
            //
            uncompressedBuffer = new byte[packetLength + 1];
            readFully(uncompressedBuffer, 0, packetLength);
        }

        if ((this.uncompressedPacket != null)
                && (this.pos < this.uncompressedPacket.length)) {
            int remainingLength = this.uncompressedPacket.length - this.pos;

            byte[] combinedBuffer = new byte[remainingLength
                + uncompressedBuffer.length];
            System.arraycopy(this.uncompressedPacket, this.pos, combinedBuffer, 0,
                remainingLength);
            System.arraycopy(uncompressedBuffer, 0, combinedBuffer,
                remainingLength, uncompressedBuffer.length);
            uncompressedBuffer = combinedBuffer;
        }

        this.uncompressedPacket = uncompressedBuffer;
        this.pos = 0;

        return;
    }

    /**
     * Determines if another packet needs to be read from the server
     * to be able to read numBytes from the stream.
     *
     * @param numBytes the number of bytes to be read
     * @throws IOException if an I/O error occors.
     */
    private void getNextPacketIfRequired(int numBytes)
        throws IOException {
        if ((this.uncompressedPacket == null)
                || ((this.pos + numBytes) > this.uncompressedPacket.length)) {
            getNextPacketFromServer();
        }
    }

    private final int readFully(byte[] b, int off, int len)
        throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = this.in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }

        return n;
    }
}
