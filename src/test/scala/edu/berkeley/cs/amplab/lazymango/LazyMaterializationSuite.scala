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
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceRecord, SequenceDictionary }
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import org.bdgenomics.adam.cli.DictionaryCommand

import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.scalatest.FunSuite
import org.scalatest.Matchers

class LazyMaterializationSuite extends LazyFunSuite  {

	def getDataFromBamFile(file: String, viewRegion: ReferenceRegion): RDD[(ReferenceRegion, AlignmentRecord)] = {
		val readsRDD: RDD[AlignmentRecord] = sc.loadIndexedBam(file, viewRegion)
		readsRDD.keyBy(ReferenceRegion(_))
	}

	// TODO: reconsider placement of sd
	val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L),
	    SequenceRecord("chrM", 2000L), 
	    SequenceRecord("chr3", 2000L))) 

	sparkTest("get data from lazy materialization structure") {
		val bamFile = "./mouse_chrM.bam"
	    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc)
	    val region = new ReferenceRegion("chrM", 0L, 1050L)
	    val results:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")

	}

	sparkTest("assert the data pulled from a file is the same") {
		val bamFile = "./mouse_chrM.bam"
	    var lazyMat = LazyMaterialization(bamFile, sc)
	    val region = new ReferenceRegion("chrM", 0L, 1000L)
		var startTime = System.currentTimeMillis

	    var results:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")		
	    var lazySize = results.get.get(region).get(0)._2.length
	   // results.get.get(0).map(rec => rec._2)
   		var endTime = System.currentTimeMillis
		var diff = (endTime - startTime)
		println("query1: " + diff)

		startTime = System.currentTimeMillis
		lazyMat.get(region, "person1")
  		endTime = System.currentTimeMillis
		diff = (endTime - startTime)
		println("query2: " + diff)

		// startTime = System.currentTimeMillis
		// lazyMat.get(region, "person1")
  // 		endTime = System.currentTimeMillis
		// diff = (endTime - startTime)
		// println("query3: " + diff)

		// startTime = System.currentTimeMillis
		// lazyMat.get(region, "person1")
  // 		endTime = System.currentTimeMillis
		// diff = (endTime - startTime)
		// println("query4: " + diff)
		// startTime = System.currentTimeMillis
	 //    val filedata = getDataFromBamFile(bamFile, region)
	 //    endTime = System.currentTimeMillis
	 //    diff = endTime - startTime
	 //    println("direct load:" + diff)
	 //    val data = filedata.map(rec => rec._2)
	 //    val dataSize = data.collect().length

	 //    println("final data sizes", lazySize, dataSize)
	 //    assert(dataSize == lazySize)
	}

	// sparkTest("Performance Test 1, region of 0-1000") {
	// 	val newChunkSize = 1001L
	// 	val region = new ReferenceRegion("chrM", 0L, 100L)
	// 	var startTime = System.currentTimeMillis
	// 	var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
	// 	val results1:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")
	// 	println("Query 1 datasize: ", results1.get.get(region).get(0)._2.length) 
	// 	var endTime = System.currentTimeMillis
	// 	var diff = (endTime - startTime)
	// 	println("query1: " + diff)

	// 	startTime = System.currentTimeMillis
	// 	val results2:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")
	// 	println("Query 1 datasize: ", results2.get.get(region).get(0)._2.length)
	// 	endTime = System.currentTimeMillis
	// 	diff = (endTime - startTime)
	// 	println("query2: " + diff)

	// 	startTime = System.currentTimeMillis
	// 	val results3:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")
	// 	endTime = System.currentTimeMillis
	// 	diff = (endTime - startTime)
	// 	println("query3: " + diff)


	// 	startTime = System.currentTimeMillis
	// 	val results4:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")
	// 	endTime = System.currentTimeMillis
	// 	diff = (endTime - startTime)
	// 	println("query4: " + diff)

	// 	startTime = System.currentTimeMillis
	// 	val results5:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")
	// 	endTime = System.currentTimeMillis
	// 	diff = (endTime - startTime)
	// 	println("query5: " + diff)

	// 	startTime = System.currentTimeMillis
	// 	var lazyMat2 = LazyMaterialization("./mouse_chrM.bam", sc, newChunkSize)
	// 	val results6:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat2.get(region, "person1")
	// 	endTime = System.currentTimeMillis
	// 	diff = (endTime - startTime)
	// 	println("query6: " + diff)

	// 	assert(results1.get.head._2.size == results2.get.head._2.size)

	// }

	// sparkTest("get data from different chromosomes") {
	// 	val bamFile = "./mouse_chrM.bam"
	//     var lazyMat = LazyMaterialization(bamFile, sc)
	//     val region = new ReferenceRegion("chrM", 0L, 100L)
	//     val results:  Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = lazyMat.get(region, "person1")		
	//     val lazySize = results.get.get(region).get(0)._2.length
	//    // results.get.get(0).map(rec => rec._2)
	//     val filedata = getDataFromBamFile(bamFile, region)
	//     val data = filedata.map(rec => rec._2)
	//     val dataSize = data.collect().length

	//     println("final data sizes", lazySize, dataSize)
	//     assert(dataSize == lazySize)
	// }

	// sparkTest("Get data using adam file and compare results") {

	// }

	// sparkTest("Get data from different samples at the same region") {

	// }


}
