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
	//
	// sparkTest("get data from lazy materialization structure") {
	// 	val bamFile = "./mouse_chrM_p1.bam"
	//     var lazyMat = LazyMaterialization[AlignmentRecord](sc)
	//
	//     val sample = "person1"
	//     lazyMat.loadSample(sample, bamFile)
	//     val region = new ReferenceRegion("chrM", 0L, 1050L)
	//     val results: Map[String, List[AlignmentRecord]]  = lazyMat.get(region, sample)
	//
	// }

	// sparkTest("assert the data pulled from a file is the same") {
	// 	val bamFile = "./mouse_chrM_p1.bam"
  //   val sample = "sample1"
	//
	//   var lazyMat = LazyMaterialization[AlignmentRecord](sc)
	// 	lazyMat.loadSample(sample, bamFile)
	//
	//   val region = new ReferenceRegion("chrM", 0L, 1000L)
	//
  //   var results: Map[String, List[AlignmentRecord]] = lazyMat.get(region, sample)
  //   var lazySize = results(sample).length
  //   val filedata = getDataFromBamFile(bamFile, region)
  //   val data = filedata.map(rec => rec._2)
  //   val dataSize = data.collect().length
	//
  //   assert(dataSize == lazySize)
	// }

	sparkTest("Get data from different samples at the same region") {
		val bamFile = "./mouse_chrM_p1.bam"
		val sample1 = "person1"
		val sample2 = "person2"

	    var lazyMat = LazyMaterialization[AlignmentRecord](sc)
	    val region = new ReferenceRegion("chrM", 0L, 100L)
	    lazyMat.loadSample(sample1, bamFile)
	    lazyMat.loadSample(sample2, bamFile)
	    val results1:  Map[String, List[AlignmentRecord]] = lazyMat.get(region, sample1)
	    val lazySize1 = results1(sample1).size

	    val results2:  Map[String, List[AlignmentRecord]] = lazyMat.get(region, sample2)
	    val lazySize2 = results2(sample2).size

	    assert(lazySize1 == lazySize2)
			assert(lazySize1 == 1074)
	}

	sparkTest("Get data for variants") {
		val region = new ReferenceRegion("chr1", 0L, 100L)
		val vcfFile = "./true.vcf"
		val callset = "callset1"
		var lazyMat = LazyMaterialization[Genotype](sc)
		lazyMat.loadSample(callset, vcfFile)

		val results1:  Map[String, List[Genotype]]= lazyMat.get(region, callset)
		assert(results1(callset).size == 3)
	}

}
