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

package com.github.ehiggs.spark.terasort

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext, TaskContext}

import java.util.Comparator
import scala.util.Random

/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSortComp {

  implicit val caseInsensitiveOrdering: Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def makeDataset(sc: SparkContext): RDD[(Array[Byte], Array[Byte])] = {
    val path = "/user/jiguanglizipao/terasort-400G"
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](path)
    dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dataset.foreach(_ => {})
    dataset
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:")
      println("DRIVER_MEMORY=[mem] spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "[input-file] [output-file]")
      println(" ")
      println("Example:")
      println("DRIVER_MEMORY=50g spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "/home/myuser/terasort_in /home/myuser/terasort_out")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)
    val numMapper = args(2).toInt
    val numReducer = args(3).toInt
    val numIters = if (args.length > 4) args(4).toInt else 1

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"TeraSort")
    val sc = new SparkContext(conf)

    val identPartitioner = new Partitioner {
      override def numPartitions: Int = numMapper

      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }

    val t1 = System.nanoTime()
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
      .mapPartitions(it => {
        val rng = new Random(TaskContext.getPartitionId())
        it.map(x => ((math.abs(rng.nextLong()) % numMapper).toInt, x))
      })
      .partitionBy(identPartitioner)
      .map { case (_, (k, v)) => (k, v) }
    dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dataset.localCheckpoint()
    dataset.foreach(_ => {})
    val t2 = System.nanoTime()
    println(s"Preprocess time: ${1e-9 * (t2 - t1)} sec")

    dataset.cleanShuffleDependencies(true)
    System.gc()
    System.runFinalization()

    for (_ <- 0 until numIters) {
      System.gc()
      System.runFinalization()
      val bt = System.nanoTime()
      val sorted = dataset.repartitionAndSortWithinPartitions(
        new TeraSortPartitioner(numReducer))
      val count = sorted.count()
      val et = System.nanoTime()
      println(s"Compute time: ${1e-9 * (et - bt)} sec")
      println(s"Sorted num records: ${count}")
      sorted.cleanShuffleDependencies(true)
    }

    sc.stop()
  }
}
