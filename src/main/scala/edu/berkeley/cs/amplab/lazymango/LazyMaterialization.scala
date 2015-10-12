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
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import org.bdgenomics.adam.models.ReferenceRegion

import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap


//TODO: Have it do things other than AlignmentRecord, currently just trying it out
class LazyMaterialization(filePath: String, sc: SparkContext, chunkSize: Long) {

  def this(filePath: String, sc: SparkContext) = {
    this(filePath, sc, 1000)
  }

  // TODO: could merge into 1 interval tree with nodes storing (chr, list(keys)). Storing the booleans is a waste of space
  var bookkeep: HashMap[String, IntervalTree[String, Boolean]] = new HashMap() 

	// TODO: if file/files provided does not have have all the chromosomes, how do we fetch files?
	// file path of file we're lazily materializing. Currently it's only one file
	val fp: String = filePath
	// K = interval, S = sec key (person id), V = data (alignment record)
	var intRDD: IntervalRDD[String, Interval[Long], String, AlignmentRecord] = null


  private def rememberValues(chr: String, intl: Interval[Long], ks: List[String]) = {
    // find chr in bookkeep
    if (bookkeep.keys.size != 0) {
      println(bookkeep.get("chrM").get.printNodes)
    }
    if (bookkeep.contains(chr)) {
      bookkeep(chr).insert(intl, ks.map( k => (k, true)))
    } else {
      val newTree = new IntervalTree[String, Boolean]()
      val newks = ks.map( k => (k, true))
      newTree.insert(intl, newks)
      bookkeep += ((chr, newTree))
    }
  }

  def loadadam(chr: String, intl: Interval[Long]): RDD[((String, Interval[Long]), (String, AlignmentRecord))] = {
    val pred: FilterPredicate = ((LongColumn("end") >= intl.start) && (LongColumn("start") <= intl.end))
    val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
    val data = sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj))
    data.map(rec => ((chr,new Interval(rec.start, rec.end)), ("person1", rec)))
  }

  def loadbam(chr: String, intl: Interval[Long]): RDD[((String, Interval[Long]), (String, AlignmentRecord))]  = {
    var viewRegion = ReferenceRegion(chr, intl.start, intl.end)
    val idxFile: File = new File(fp + ".bai")
    if (!idxFile.exists()) {
      val data: RDD[AlignmentRecord] = sc.loadBam(fp).filterByOverlappingRegion(viewRegion)
      data.map(rec => ((chr,new Interval(rec.start, rec.end)), ("person1", rec)))
    } else {
      val data: RDD[AlignmentRecord] = sc.loadIndexedBam(fp, viewRegion)
      data.map(rec => ((chr,new Interval(rec.start, rec.end)), ("person1", rec)))
    }
  }

  def loadFromFile(chr: String, intl: Interval[Long]): RDD[((String, Interval[Long]), (String, AlignmentRecord))]  = {
    println("LOADING FROM DISK")
    if (fp.endsWith(".adam")) {
      loadadam(chr, intl)
    } else if (fp.endsWith(".sam") || fp.endsWith(".bam")) {
      loadbam(chr, intl)
    } else {
      throw UnsupportedFileException("File type not supported")
    }
  }


  def get(chr: String, i: Interval[Long], k: String): Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = multiget(chr, i, List(k))

	/* If the RDD has not been initialized, initialize it to the first get request
	* Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
	* If it exists, call get on the IntervalRDD
	* Otherwise call put on the sections of data that don't exist
	* Here, ks, is an option of list of personids (String)
	*/
	def multiget(chr: String, i: Interval[Long], ks: List[String]): Option[Map[Interval[Long], List[(String, AlignmentRecord)]]] = {

    val intl: Interval[Long] = getChunk(i)

		if (intRDD == null) {
      val ready = loadFromFile(chr, intl)
  		intRDD = IntervalRDD(ready)
      rememberValues(chr, intl, ks)
      intRDD.multiget(chr, i, Option(ks))
		} 
    else {
      val intls = partitionChunk(intl)

      //Is this necessary? This gives additional load even when it's there. I've uncommented it for now
      // for (i <- intls) {
      //   val found: List[String] = bookkeep(chr).search(i, ks).map(k => k._1)
      //   // for all keys not in found, add to list
      //   val notFound: List[String] = ks.filterNot(found.contains(_))
      //   println("found is: " + found)
      //   println("not found is: " + notFound)
      //   println("PUTTING IN RDD")
      //   put(chr, i, notFound)
      // }
      println("GETTING FROM RDD")
      println(" ")
      val start = System.currentTimeMillis
      val returnVal = intRDD.multiget(chr, i, Option(ks))
      val end = System.currentTimeMillis
      println("TIME TO PERFORM INTERVALRDD.MULTIGET")
      println(end - start)
      returnVal
		}
	}

	/* Transparent to the user, should only be called by get if IntervalRDD.get does not return data
	* Fetches the data from disk, using predicates and range filtering
	* Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
	*/
  private def put(chr: String, intl: Interval[Long], ks: List[String]) = {

    val pred: FilterPredicate = ((LongColumn("end") >= intl.start) && (LongColumn("start") <= intl.end))
    val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
    // val loading: RDD[AlignmentRecord] = sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj))
    val loading: RDD[AlignmentRecord] = sc.loadAlignments(fp, projection = Some(proj))
		val ready: RDD[((String, Interval[Long]), (String, AlignmentRecord))] = loading.map(rec => ((chr,new Interval(rec.start, rec.end)), ("person1", rec)))    
    intRDD.multiput(ready)
  	rememberValues(chr, intl, ks)
	}

  // get block chunk of request
  private def getChunk(intl: Interval[Long]): Interval[Long] =  {
    val start = intl.start / chunkSize * chunkSize
    val end = intl.end / chunkSize * chunkSize + (chunkSize - 1)
    val interval: Interval[Long] = new Interval(start, end)
    interval
  }

  // get block chunk of request
  private def partitionChunk(intl: Interval[Long]): List[Interval[Long]] =  {
  var intls: ListBuffer[Interval[Long]] = new ListBuffer[Interval[Long]]()
    var start = intl.start / chunkSize * chunkSize
    var end = start + (chunkSize - 1)

    while(start <= intl.end) {
      intls += new Interval(start, end)
      start += chunkSize
      end += chunkSize
    }
    intls.toList
  }

}

case class UnsupportedFileException(message: String) extends Exception(message)


//Takes in a file path, contains adapters to pull in RDD[BDGFormat]
object LazyMaterialization {

  //Create a Lazy Materialization object by feeding in a file path to build an RDD from
	def apply(filePath: String, sc: SparkContext): LazyMaterialization = {
		new LazyMaterialization(filePath, sc)
	}
  def apply(filePath: String, sc: SparkContext, chunkSize: Long): LazyMaterialization = {
    new LazyMaterialization(filePath, sc, chunkSize)
  }
}