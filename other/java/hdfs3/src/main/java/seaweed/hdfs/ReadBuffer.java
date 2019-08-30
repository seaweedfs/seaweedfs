/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package seaweed.hdfs;

import java.util.concurrent.CountDownLatch;

class ReadBuffer {

  private SeaweedInputStream stream;
  private long offset;                   // offset within the file for the buffer
  private int length;                    // actual length, set after the buffer is filles
  private int requestedLength;           // requested length of the read
  private byte[] buffer;                 // the buffer itself
  private int bufferindex = -1;          // index in the buffers array in Buffer manager
  private ReadBufferStatus status;             // status of the buffer
  private CountDownLatch latch = null;   // signaled when the buffer is done reading, so any client
  // waiting on this buffer gets unblocked

  // fields to help with eviction logic
  private long timeStamp = 0;  // tick at which buffer became available to read
  private boolean isFirstByteConsumed = false;
  private boolean isLastByteConsumed = false;
  private boolean isAnyByteConsumed = false;

  public SeaweedInputStream getStream() {
    return stream;
  }

  public void setStream(SeaweedInputStream stream) {
    this.stream = stream;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getRequestedLength() {
    return requestedLength;
  }

  public void setRequestedLength(int requestedLength) {
    this.requestedLength = requestedLength;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(byte[] buffer) {
    this.buffer = buffer;
  }

  public int getBufferindex() {
    return bufferindex;
  }

  public void setBufferindex(int bufferindex) {
    this.bufferindex = bufferindex;
  }

  public ReadBufferStatus getStatus() {
    return status;
  }

  public void setStatus(ReadBufferStatus status) {
    this.status = status;
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public boolean isFirstByteConsumed() {
    return isFirstByteConsumed;
  }

  public void setFirstByteConsumed(boolean isFirstByteConsumed) {
    this.isFirstByteConsumed = isFirstByteConsumed;
  }

  public boolean isLastByteConsumed() {
    return isLastByteConsumed;
  }

  public void setLastByteConsumed(boolean isLastByteConsumed) {
    this.isLastByteConsumed = isLastByteConsumed;
  }

  public boolean isAnyByteConsumed() {
    return isAnyByteConsumed;
  }

  public void setAnyByteConsumed(boolean isAnyByteConsumed) {
    this.isAnyByteConsumed = isAnyByteConsumed;
  }

}
