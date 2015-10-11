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
	    val results:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", new Interval(0L, 700L), "person1")
	}

	sparkTest("reget data from lazy materialization structure") {
	    var lazyMat = LazyMaterialization("./mouse_chrM.bam", sc)
	    val results:  Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = lazyMat.get("chrM", new Interval(0L, 700L), "person1")

		val results2 = lazyMat.get("chrM", new Interval(600L, 800L), "person1")

	}


	test("look up intervals larger than chunk size") {
		assert(0 == 1)

	}

}