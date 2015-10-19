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

package edu.berkeley.cs.amplab.lazymango

import com.github.akmorrow13.intervaltree._
import edu.berkeley.cs.amplab.spark.intervalrdd._
import scala.reflect.ClassTag
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.scalatest.FunSuite
import org.scalatest.Matchers

class LazyMaterializationSuite extends ADAMFunSuite  {

	sparkTest("get data from lazy materialization structure") {
	    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc)
	    val results:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", new Interval(0L, 1050L), "person1")
	}

  sparkTest("Performance Test 1, region of 0-1000") {
    val newChunkSize = 1001L
    val intl = new Interval[Long](0L, 1000L)
    var startTime = System.currentTimeMillis
    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
    val results1:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")  
    var endTime = System.currentTimeMillis
    var diff = (endTime - startTime)
    println("query1: " + diff)

    startTime = System.currentTimeMillis
    val results2:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query2: " + diff)

    startTime = System.currentTimeMillis
    val results3:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query3: " + diff)


    startTime = System.currentTimeMillis
    val results4:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query4: " + diff)

    startTime = System.currentTimeMillis
    val results5:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query5: " + diff)

    startTime = System.currentTimeMillis
    var lazyMat2 = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
    val results6:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat2.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query6: " + diff)

    assert(results1.get.head._2.size == results2.get.head._2.size)

  }


  sparkTest("Performance Test 2: Chunk size < interval, region of 0-1000") {
    val newChunkSize = 500L
    val intl = new Interval[Long](0L, 1000L)
    var startTime = System.currentTimeMillis
    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
    val results1:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")  
    var endTime = System.currentTimeMillis
    var diff = (endTime - startTime)
    println("query1: " + diff)

    startTime = System.currentTimeMillis
    val results2:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query2: " + diff)

    startTime = System.currentTimeMillis
    val results3:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query3: " + diff)


    startTime = System.currentTimeMillis
    val results5:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query4: " + diff)

    startTime = System.currentTimeMillis
    var lazyMat2 = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
    val results6:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat2.get("chrM", intl, "person1")
    endTime = System.currentTimeMillis
    diff = (endTime - startTime)
    println("query5: " + diff)

    assert(results1.get.head._2.size == results2.get.head._2.size)

  }

	sparkTest("configurable chunk size") {
		val newChunkSize = 3000
		val intl = new Interval[Long](0L, 700L)

	 	var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
	    val results1:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")	
	
 		var defaultLazyMat = LazyMaterialization("./mouse_chrM.bam", sc)
	    val results2:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = defaultLazyMat.get("chrM", intl, "person1")

		assert(results1.get.head._2.size == results2.get.head._2.size)

	}

	sparkTest("reget data from lazy materialization structure") {
	    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc)

	    val intl1: Interval[Long] = new Interval[Long](10L, 150L)
	    val results:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl1, "person1")
	    val intervals: (Interval[Long], List[(String, AlignmentRecord)])  = results.get.head

	    intervals._2.foreach(k => assert(intl1.overlaps(new Interval(k._2.start, k._2.end))))

	    val intl2: Interval[Long] = new Interval[Long](600L, 800L)
		val results2: Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl2, "person1")
	    // for each element in results2, check that they are between intl2
	    val intervals2: (Interval[Long], List[(String, AlignmentRecord)])  = results2.get.head

	    intervals2._2.foreach(k => {
	    	val testIntl = new Interval[Long](k._2.start, k._2.end)
	    	val overlaps = intl2.overlaps(new Interval(k._2.start, k._2.end))
	    	if (!overlaps) {
	    		println(testIntl)
	    		println("overlap was not in range")
	    	}
	    })

	    //intervals2._2.foreach(k => assert(intl2.overlaps(new Interval(k._2.start, k._2.end))))
	}

	sparkTest("look up intervals larger than chunk size") {

	 	val intl = new Interval[Long](0L, 1500L)
	    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc)
	    val results:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl, "person1")

	    val intervals: List[(String, AlignmentRecord)]  = results.get.head._2

	    intervals.foreach(k => assert(intl.overlaps(new Interval(k._2.start, k._2.end))))

	 	val intl1 = new Interval[Long](0L, 999L)
	 	val intl2 = new Interval[Long](1000L, 1500L)
	 	val results1:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl1, "person1")
	    val results2:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", intl2, "person1")

		val combResults: List[(String, AlignmentRecord)] = results1.get.head._2 ::: results2.get.head._2

		val filteredResults: List[(String, AlignmentRecord)] = combResults.distinct

		assert(intervals.distinct.size ==filteredResults.distinct.size)

	}

    sparkTest("look up from ADAM file") {
        assert(0 == 1)
    }

}