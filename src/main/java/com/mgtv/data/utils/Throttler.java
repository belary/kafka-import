/*
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

package com.mgtv.data.utils;

/** kafka写入的速率调节器
 *  A data throttler that
 *  controls the rate at which data is written out to Kafka.
 *
 *  e.g.
 *  1. 每秒限速50条
 *  则 ratePerSubtask=50.0,  throttleBatchSize=3 , nanosPerBatch=60000000
 *
 *  2. 每秒限速20条
 *  则 ratePerSubtask=20.0,  throttleBatchSize=2 , nanosPerBatch=100000000
 *
 *  3. 每秒限速1w条
 *  ratePerSubtask=10000.0， throttleBatchSize=501， nanosPerBatch=50100000
 *  */
final class Throttler {

  // 每批发送的记录条数
  private final long throttleBatchSize;

  // 每批的发送消耗时长 纳秒
  private final long nanosPerBatch;

  // 每批次结束的发送绝对计时
  private long endOfNextBatchNanos;

  // 当前批次
  private int currentBatch;

  Throttler(long maxRecordsPerSecond) {
    if (maxRecordsPerSecond == -1) {
      // unlimited speed
      throttleBatchSize = -1;
      nanosPerBatch = 0;
      endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
      currentBatch = 0;
      return;
    }

    // 每秒的最多发送记录数
    final float ratePerSubtask = (float) maxRecordsPerSecond;

    if (ratePerSubtask >= 10000) {
      // 每秒超过1w，每批次的大小则通过将每秒的任务拆分为500等份
      // high rates: all throttling in intervals of 2ms
      throttleBatchSize = (int) ratePerSubtask / 500;

      // 每批次2ms处理时长
      nanosPerBatch = 2_000_000L;
    } else {
      // 每秒未超过1w，每批次的大小的则通过将每秒的任务拆分为20等份
      throttleBatchSize = ((int) (ratePerSubtask / 20)) + 1;

      // 每批次处理的纳秒时长 (每秒生产条数确定的情况下) =  1秒 / 每秒的最多发送记录数
      nanosPerBatch = ((int) (1_000_000_000L / ratePerSubtask)) * throttleBatchSize;
    }
    this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
    this.currentBatch = 0;
  }

  void throttle() throws InterruptedException {
    if (throttleBatchSize == -1) {
      return;
    }
    if (++currentBatch != throttleBatchSize) {
      return;
    }
    currentBatch = 0;

    final long now = System.nanoTime();
    final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

    // 判断每批次的任务是否提前执行完成
    if (millisRemaining > 0) {
      // 则休眠一个批次的中剩余的时间
      endOfNextBatchNanos += nanosPerBatch;
      Thread.sleep(millisRemaining);
    } else {
      // 每批次执行超时：则将下批次的执行时长设置为：当前时间+每批次执行时长
      endOfNextBatchNanos = now + nanosPerBatch;
    }
  }

  public static void main(String[] args) {

    float ratePerSubtask = (float)10000;
    long throttleBatchSize = ((int) (ratePerSubtask/20))  + 1;
    long nanosPerBatch = ((int) (1_000_000_000L / ratePerSubtask)) * throttleBatchSize;

    System.out.println("ratePerSubtask=" + ratePerSubtask);
    System.out.println("throttleBatchSize=" + throttleBatchSize);
    System.out.println("nanosPerBatch=" + nanosPerBatch);
    System.out.println("System.nanoTime()=" + System.nanoTime());
  }
}
