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

import java.io.File

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
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import org.bdgenomics.adam.models.ReferenceRegion

import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap


//TODO: Have it do things other than AlignmentRecord, currently just trying it out
class LazyMaterialization(filePath: String, sc: SparkContext, dict: SequenceDictionary, chunkSize: Long) {

  def this(filePath: String, sc: SparkContext, dict: SequenceDictionary) = {
    this(filePath, sc, dict, 1000)
  }

  // TODO: could merge into 1 interval tree with nodes storing (chr, list(keys)). Storing the booleans is a waste of space
  var bookkeep: HashMap[String, IntervalTree[String, Boolean]] = new HashMap() 

	// TODO: if file/files provided does not have have all the chromosomes, how do we fetch files?
	// file path of file we're lazily materializing. Currently it's only one file
	val fp: String = filePath
	// K = interval, S = sec key (person id), V = data (alignment record)
	var intRDD: IntervalRDD[String, List[AlignmentRecord]] = null


  private def rememberValues(region: ReferenceRegion, ks: List[String]) = {
    // find chr in bookkeep
    if (bookkeep.keys.size != 0) {
      println(bookkeep.get("chrM").get.printNodes)
    }
    if (bookkeep.contains(region.referenceName)) {
      bookkeep(region.referenceName).insert(region, ks.map( k => (k, true)))
    } else {
      val newTree = new IntervalTree[String, Boolean]()
      val newks = ks.map( k => (k, true))
      newTree.insert(region, newks)
      bookkeep += ((region.referenceName, newTree))
    }
  }

  def loadadam(region: ReferenceRegion): RDD[AlignmentRecord] = {
    val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end))
    val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
    sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj))
  }

  def loadbam(region: ReferenceRegion): RDD[AlignmentRecord]  = {
    val idxFile: File = new File(fp + ".bai")
    if (!idxFile.exists()) {
      sc.loadBam(fp).filterByOverlappingRegion(region)
    } else {
      sc.loadIndexedBam(fp, region)
    }
  }

  def loadFromFile(region: ReferenceRegion): RDD[AlignmentRecord]  = {
    if (fp.endsWith(".adam")) {
      loadadam(region)
    } else if (fp.endsWith(".sam") || fp.endsWith(".bam")) {
      loadbam(region)
    } else {
      throw UnsupportedFileException("File type not supported")
    }
  }

  def get(region: ReferenceRegion, k: String): Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = multiget(region, List(k))

	/* If the RDD has not been initialized, initialize it to the first get request
	* Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
	* If it exists, call get on the IntervalRDD
	* Otherwise call put on the sections of data that don't exist
	* Here, ks, is an option of list of personids (String)
	*/
	def multiget(region: ReferenceRegion, ks: List[String]): Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = {

    val matRegion: ReferenceRegion = getChunk(region)

		if (intRDD == null) {
      val ready = loadFromFile(matRegion)
      val data = Array((region, ("person1", ready.collect.toList)))
      val rdd = sc.parallelize(data)
      println("initial records to be added to intervalrdd")
  		intRDD = IntervalRDD(rdd, dict)
      rememberValues(matRegion, ks)
      filterByRegion(intRDD.multiget(region, Option(ks)))

		} 
    else {
      val regions = partitionChunk(matRegion)

      for (r <- regions) {
        val found: List[String] = bookkeep(r.referenceName).search(r, ks).map(k => k._1)
        // for all keys not in found, add to list
        val notFound: List[String] = ks.filterNot(found.contains(_))
        put(r, notFound)
      }
      intRDD.multiget(region, Option(ks))
		}
	}

	/* Transparent to the user, should only be called by get if IntervalRDD.get does not return data
	* Fetches the data from disk, using predicates and range filtering
	* Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
	*/
  private def put(region: ReferenceRegion, ks: List[String]) = {

    val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end))
    val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
    // val loading: RDD[AlignmentRecord] = sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj))
    val loading: RDD[AlignmentRecord] = sc.loadAlignments(fp, projection = Some(proj))
    val ready: RDD[(ReferenceRegion, (String, List[AlignmentRecord]))]  = sc.parallelize(Array((region, ("person1", loading.collect.toList))))
    intRDD.multiput(ready)
  	rememberValues(region, ks)
	}

  // get block chunk of request
  private def getChunk(region: ReferenceRegion): ReferenceRegion = {
    val start = region.start / chunkSize * chunkSize
    val end = region.end / chunkSize * chunkSize + (chunkSize - 1)
    new ReferenceRegion(region.referenceName, start, end)
  }

  // get block chunk of request
  private def partitionChunk(region: ReferenceRegion): List[ReferenceRegion] = {
    var regions: ListBuffer[ReferenceRegion] = new ListBuffer[ReferenceRegion]()
    var start = region.start / chunkSize * chunkSize
    var end = start + (chunkSize - 1)

    while(start <= region.end) {
      regions += new ReferenceRegion(region.referenceName, start, end)
      start += chunkSize
      end += chunkSize
    }
    regions.toList
  }

  private def filterByRegion(map: Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]]): Option[Map[ReferenceRegion, List[(String, List[AlignmentRecord])]]] = {
    // TODO: filter input data by region
    val data = map.get
    data.foreach((rec: (ReferenceRegion, List[(String, List[AlignmentRecord])])) => (rec._1, filterAlignmentRecords(rec._1, rec._2)))
    Option(data)
  }

  private def filterAlignmentRecords(region: ReferenceRegion, data: List[(String, List[AlignmentRecord])]): List[(String, List[AlignmentRecord])] = {
    // TODO: this doesnt return filtered data
    data.foreach(rec => (rec._1, rec._2.filter(r => r.start < region.end && r.end > region.start)))
    data
  }

}

case class UnsupportedFileException(message: String) extends Exception(message)


//Takes in a file path, contains adapters to pull in RDD[BDGFormat]
object LazyMaterialization {

  //Create a Lazy Materialization object by feeding in a file path to build an RDD from
	def apply(filePath: String, sc: SparkContext, dict: SequenceDictionary): LazyMaterialization = {
		new LazyMaterialization(filePath, sc, dict)
	}
  def apply(filePath: String, sc: SparkContext, dict: SequenceDictionary, chunkSize: Long): LazyMaterialization = {
    new LazyMaterialization(filePath, sc, dict, chunkSize)
  }
}