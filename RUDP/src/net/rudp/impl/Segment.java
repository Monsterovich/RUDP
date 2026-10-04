/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2009, Adrian Granados (agranados@ihmc.us)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS AND CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp.impl;

public abstract class Segment
{
    public static final int RUDP_VERSION = 1;
    public static final int RUDP_HEADER_LEN = 6;

    public static final byte SYN_FLAG = (byte) 0x80;
    public static final byte ACK_FLAG = (byte) 0x40;
    public static final byte EAK_FLAG = (byte) 0x20;
    public static final byte RST_FLAG = (byte) 0x10;
    public static final byte NUL_FLAG = (byte) 0x08;
    public static final byte CHK_FLAG = (byte) 0x04;
    public static final byte FIN_FLAG = (byte) 0x02;


    protected Segment()
    {
        _nretx = 0;
        _ackn = -1;
        _deadline = Long.MAX_VALUE;
    }

    public abstract String type();

    public int flags()
    {
        return _flags;
    }

    public int seq()
    {
        return _seqn;
    }

    public int length()
    {
        return _hlen;
    }

    public void setAck(int ackn)
    {
        _flags = _flags | ACK_FLAG;
        _ackn = ackn;
    }

    public int getAck()
    {
        if ((_flags & ACK_FLAG) == ACK_FLAG) {
            return _ackn;
        }

        return -1;
    }

    public int getRetxCounter()
    {
        return _nretx;
    }

    public void setRetxCounter(int n)
    {
        _nretx = n;
    }

    public byte[] getBytes()
    {
        byte[] buffer = new byte[length()];
        writeTo(buffer, 0);
        return buffer;
    }

    /**
     * Serializes this segment into an existing buffer, avoiding the
     * per-packet byte[] allocation that getBytes() would incur.
     *
     * @param buffer the destination buffer.
     * @param off    the offset in the destination buffer.
     * @return the number of bytes written.
     */
    public int writeTo(byte[] buffer, int off)
    {
        buffer[off] = (byte) (_flags & 0xFF);
        buffer[off+1] = (byte) (_hlen & 0xFF);
        buffer[off+2] = (byte) (_seqn & 0xFF);
        buffer[off+3] = (byte) (_ackn & 0xFF);
        buffer[off+4] = 0; /* checksum (unused) */
        buffer[off+5] = 0;

        return RUDP_HEADER_LEN;
    }

    /**
     * Records the instant at which this segment went (re)transmitted and
     * the instant at which it becomes eligible for a timeout retransmission.
     */
    public void markSent(long nowMillis, int rtoMillis)
    {
        _sentTime = nowMillis;
        _deadline = nowMillis + rtoMillis;
    }

    public long sentTime()
    {
        return _sentTime;
    }

    public long deadline()
    {
        return _deadline;
    }

    /**
     * Records one more timeout on this segment, doubling its retransmission
     * backoff up to 'shift' doublings. The multiplier itself is applied by the
     * caller when it stamps the next deadline (see ReliableSocket.rtoFor), so
     * that the base RTO and its bounds stay policy of the socket.
     */
    public void backOffRto(int shift)
    {
        if (_rtoShift < shift) {
            _rtoShift++;
        }
    }

    /**
     * Number of exponential backoff doublings accumulated by this segment.
     */
    public int rtoShift()
    {
        return _rtoShift;
    }

    public void clearBackOff()
    {
        _rtoShift = 0;
    }

    /**
     * Flags this segment as retransmitted at least once. Karn's algorithm:
     * such a segment must never be used to sample the round trip time.
     */
    public void markRetransmitted()
    {
        _retransmitted = true;
    }

    public boolean wasRetransmitted()
    {
        return _retransmitted;
    }

    public void markAcked()
    {
        _acked = true;
    }

    public boolean isAcked()
    {
        return _acked;
    }

    public String toString()
    {
        return type() +
        " [" +
        " SEQ = " + seq() +
        ", ACK = " + ((getAck() >= 0) ? ""+getAck() : "N/A") +
        ", LEN = " + length() +
        " ]";
    }

    public static Segment parse(byte[] bytes)
    {
        return Segment.parse(bytes, 0, bytes.length);
    }

    public static Segment parse(byte[] bytes, int off, int len)
    {
        Segment segment = null;

        /*
         * Checked here rather than left to the field reads below: a caller
         * that hands over a buffer too short for the header or for the length
         * it claims must be told the segment is invalid, not get an
         * ArrayIndexOutOfBoundsException out of a byte access and mistake it
         * for a corrupted datagram. A len shorter than the buffer is fine -
         * that is a segment followed by unrelated bytes.
         */
        if (off < 0 || len < RUDP_HEADER_LEN || off > bytes.length - len) {
            throw new IllegalArgumentException("Invalid segment");
        }

        int flags = bytes[off];
        if ((flags & SYN_FLAG) != 0) {
            segment = new SYNSegment();
        }
        else if ((flags & NUL_FLAG) != 0) {
            segment = new NULSegment();
        }
        else if ((flags & EAK_FLAG) != 0) {
            /* An EAK always carries at least one out-of-sequence ack number. */
            if (len <= RUDP_HEADER_LEN) {
                throw new IllegalArgumentException("Invalid EAK segment");
            }

            segment = new EAKSegment();
        }
        else if ((flags & RST_FLAG) != 0) {
            segment = new RSTSegment();
        }
        else if ((flags & FIN_FLAG) != 0) {
            segment = new FINSegment();
        }
        else if ((flags & ACK_FLAG) != 0) { /* always process ACKs or Data segments last */
            if (len == RUDP_HEADER_LEN) {
                segment = new ACKSegment();
            }
            else {
                segment = new DATSegment();
            }
        }

        if (segment == null) {
            throw new IllegalArgumentException("Invalid segment");
        }

        segment.parseBytes(bytes, off, len);
        return segment;
    }

    /*
     *  RUDP Header
     *
     *   0 1 2 3 4 5 6 7 8            15
     *  +-+-+-+-+-+-+-+-+---------------+
     *  |S|A|E|R|N|C| | |    Header     |
     *  |Y|C|A|S|U|H|0|0|    Length     |
     *  |N|K|K|T|L|K| | |               |
     *  +-+-+-+-+-+-+-+-+---------------+
     *  |  Sequence #   +   Ack Number  |
     *  +---------------+---------------+
     *  |            Checksum           |
     *  +---------------+---------------+
     *
     */
    protected void init(int flags, int seqn, int len)
    {
        _flags = flags;
        _seqn = seqn;
        _hlen = len;
    }

    protected void parseBytes(byte[] buffer, int off, int len)
    {
        _flags = (buffer[off] & 0xFF);
        _hlen  = (buffer[off+1] & 0xFF);
        _seqn  = (buffer[off+2] & 0xFF);
        _ackn  = (buffer[off+3] & 0xFF);
    }

    private int _flags; /* Control flags field */
    private int _hlen;   /* Header length field */
    private int _seqn;  /* Sequence number field */
    private int _ackn;  /* Acknowledgment number field */

    private int _nretx; /* Retransmission counter */

    private long _sentTime;      /* Wall clock of the last transmission */
    private long _deadline;      /* Wall clock at which a timeout retransmission is due */
    private int  _rtoShift;      /* Exponential backoff doublings for the retransmission timeout */
    private boolean _retransmitted;
    private boolean _acked;
}
