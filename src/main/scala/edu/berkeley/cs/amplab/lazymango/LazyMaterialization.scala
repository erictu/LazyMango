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
// import org.bdgenomics.adam.models._
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import scala.collection.mutable.ListBuffer


//TODO: Have it do things other than AlignmentRecord, currently just trying it out
class LazyMaterialization(input: RDD[(Interval[Long], (String, AlignmentRecord))], filePath: String, sc: SparkContext) {
	// chromosome, whether the interval for that chromosome exists
	val bookkeep: IntervalTree[Long, Boolean] = new IntervalTree[Long, Boolean]()

	// TODO: if file/files provided does not have have all the chromosomes, how do we fetch files?
	// file path of file we're lazily materializing. Currently it's only one file
	val fp: String = filePath
	// TODO: This correct types for alignment record?
	val intRDD: IntervalRDD[Interval[Long], String, AlignmentRecord] = IntervalRDD(input)



	/* Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
	* If it exists, call get on the IntervalRDD
	* Otherwise call put on the sections of data that don't exist
	*/
  	def multiget(chr: String, intl: Interval[Long], ks: Option[List[String]]): Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = {
  		var data = intRDD.multiget(chr, intl, ks)
  		data match {
  			case Some(_) => {
  				return data
  			}
  			case None => put(chr, intl, ks)
  		}
  	}

  	/* Transparent to the user, should only be called by get if IntervalRDD.get does not return data
  	* Fetches the data from disk, using predicates and range filtering
  	* Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
  	*/
    private def put(chr: String, intl: Interval[Long], ks: Option[List[String]]): Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = {
    	// Fetching the specific data that we want from disk
        val pred: FilterPredicate = ((LongColumn("end") >= intl.start) && (LongColumn("start") <= intl.end))
        val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
        val loading = sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj))
  		val ready: RDD[(Interval[Long], (String, AlignmentRecord))] = loading.map(rec => (new Interval(rec.start, rec.end), ("person1", rec)))
    	intRDD.multiput(chr, intl, ready)
  		multiget(chr, intl, ks)
  	}

}

//Takes in a file path, contains adapters to pull in RDD[BDGFormat]
object LazyMaterialization {

  	var conf = new SparkConf(false)
  	var sc = new SparkContext("local", "test", conf)

  	//Create a Lazy Materialization object by feeding in a file to build an RDD from
	def apply(fp: String): LazyMaterialization = {
		var returned: RDD[(Interval[Long], (String, AlignmentRecord))] = null
		//TODO: implement pattern matching, for different adapters
		//TODO: load in multiple files?
		if (fp.endsWith(".adam")) {
			println("Trying to load alignments")
	 		val loading: RDD[AlignmentRecord] = sc.loadParquetAlignments(fp) //TODO: K needs to match types of BDGFormats
	 		returned = loading.map(rec => (new Interval(rec.start, rec.end), ("person1", rec)))
		}
		new LazyMaterialization(returned, fp, sc)
	}
}