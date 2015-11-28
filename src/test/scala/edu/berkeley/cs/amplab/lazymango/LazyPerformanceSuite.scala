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

class LazyPerformanceSuite extends LazyFunSuite  {

	def getDataFromBamFile(file: String, viewRegion: ReferenceRegion): RDD[(ReferenceRegion, AlignmentRecord)] = {
		val readsRDD: RDD[AlignmentRecord] = sc.loadIndexedBam(file, viewRegion)
		readsRDD.keyBy(ReferenceRegion(_))
	}
	val region = new ReferenceRegion("chrM", 0L, 100L)
	val sampleCount = 10
	val bamFile = "./mouse_chrM_p1.bam"

	sparkTest("Get data from different regions of 1000 samples") {
		var samples = List[String]()
		var lazyMat = LazyMaterialization[AlignmentRecord](sc)

		var i = 0

		// for loop execution with a range
		for( i <- 0 to sampleCount) {
			val id = "sample".concat(i.toString)
			samples = samples :+ id
		}

		// // load 1000 samples into beer
		for( i <- 0 to sampleCount ) {
			lazyMat.loadSample(samples(i), bamFile)
		}

		val sampleSize = 1074

		// load data from files
		for( i <- 0 to sampleCount ) {
			val results1:  Map[String, List[AlignmentRecord]] = lazyMat.get(region, samples(i))
	    assert ( results1(samples(i)).size == sampleSize )
		}
	}

	sparkTest("get data of subsequent calls using file get") {
		var i = 0
		// for loop execution with a range
		for( i <- 0 to sampleCount) {
			getDataFromBamFile(bamFile, region)
		}
	}

}
