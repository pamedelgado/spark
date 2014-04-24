/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.output

import scala.collection.mutable

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import redis.clients.jedis.Jedis

class RedisHashOutputFormat extends OutputFormat[Writable, Writable] {
  // TODO convert to spark conf somehow
  val redisHosts = "spark.redishashoutputformat.hosts"
  val redisHostKey = "spark.redishashinputformat.key"

  def getRecordWriter(job: TaskAttemptContext) = {
    val hashKey = job.getConfiguration.get(redisHostKey)
    val csvHosts = job.getConfiguration.get(redisHosts)
    new RedisHashRecordWriter(hashKey, csvHosts)
  }

  class RedisHashRecordWriter(hashKey: String, hosts: String) extends RecordWriter[Writable, Writable] {

    private val jedisMap = new mutable.HashMap[Integer, Jedis]()

    var i = 0
    for (host <- hosts.split(",")) {
      val jedis = new Jedis(host)
      jedis.connect()
      jedisMap.put(i, jedis)
      i += 1
    }

    def write(key: Writable, value: Writable) {
      val j = jedisMap(Math.abs(key.hashCode()) % jedisMap.size)
      j.hset(hashKey, key.toString, value.toString)
    }

    def close(context: TaskAttemptContext) {
      for (jedis <- jedisMap.values) {
        jedis.disconnect()
      }
    }
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    // noop does nothing
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    new NullOutputFormat[Text, Text]().getOutputCommitter(context)
  }
}
