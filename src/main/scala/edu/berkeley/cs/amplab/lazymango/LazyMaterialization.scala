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
import scala.reflect.{classTag, ClassTag}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.projections.{ Projection, VariantField, AlignmentRecordField, GenotypeField, NucleotideContigFragmentField, FeatureField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature, Genotype, GenotypeAllele, NucleotideContigFragment }

import org.bdgenomics.adam.models.ReferenceRegion

import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap

class LazyMaterialization[T: ClassTag](sc: SparkContext, chunkSize: Long)  extends Serializable with Logging {

  var dict: SequenceDictionary = null

  def this(sc: SparkContext) = {
    // TODO sequence dictionary from other types besides Alignment record?
    this(sc, 1000)
  }

  def setDictionary(filePath: String) {
    dict = sc.adamDictionaryLoad[AlignmentRecord](filePath)
  }

  // Stores location of sample at a given filepath
  def loadSample(k: String, filePath: String) {
      if (dict == null) {
        setDictionary(filePath)
      }
      fileMap += ((k, filePath))
  }

  // Keeps track of sample ids and corresponding files
  private var fileMap: HashMap[String, String] = new HashMap()

  // TODO: could merge into 1 interval tree with nodes storing (chr, list(keys)). Storing the booleans is a waste of space
  private var bookkeep: HashMap[String, IntervalTree[String, Boolean]] = new HashMap() 

	// K = interval, S = sec key (person id), V = data (alignment record)
	var intRDD: IntervalRDD[String, List[T]] = null


  private def rememberValues(region: ReferenceRegion, ks: List[String]) = {
    if (bookkeep.contains(region.referenceName)) {
      bookkeep(region.referenceName).insert(region, ks.map( k => (k, true)))
    } else {
      val newTree = new IntervalTree[String, Boolean]()
      val newks = ks.map( k => (k, true))
      newTree.insert(region, newks)
      bookkeep += ((region.referenceName, newTree))
    }
  }

  private def loadadam(fp: String, region: ReferenceRegion): RDD[T] = {
    val isAlignmentRecord = classOf[AlignmentRecord].isAssignableFrom(classTag[T].runtimeClass)
    val isVariant = classOf[Genotype].isAssignableFrom(classTag[T].runtimeClass)
    val isFeature = classOf[Feature].isAssignableFrom(classTag[T].runtimeClass)
    val isNucleotideFrag = classOf[NucleotideContigFragment].isAssignableFrom(classTag[T].runtimeClass)

    if (isAlignmentRecord) {
      val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end))
      val proj = Projection(AlignmentRecordField.contig, AlignmentRecordField.readName, AlignmentRecordField.start, AlignmentRecordField.end, AlignmentRecordField.sequence, AlignmentRecordField.cigar, AlignmentRecordField.readNegativeStrand, AlignmentRecordField.readPaired)
      sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj)).asInstanceOf[RDD[T]]
    } else if (isVariant) {
      val pred: FilterPredicate = ((LongColumn("variant.end") >= region.start) && (LongColumn("variant.start") <= region.end))
      val proj = Projection(GenotypeField.variant, GenotypeField.alleles)
      sc.loadParquetGenotypes(fp, predicate = Some(pred), projection = Some(proj)).asInstanceOf[RDD[T]]
    } else if (isFeature) {
      val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end))
      val proj = Projection(FeatureField.contig, FeatureField.featureId, FeatureField.featureType, FeatureField.start, FeatureField.end)
      sc.loadParquetAlignments(fp, predicate = Some(pred), projection = Some(proj)).asInstanceOf[RDD[T]]
    } else if(isNucleotideFrag) {
      val pred: FilterPredicate = ((LongColumn("fragmentStartPosition") >= region.start) && (LongColumn("fragmentStartPosition") <= region.end))
      sc.loadParquetFragments(fp, predicate = Some(pred)).asInstanceOf[RDD[T]]
    } else {
      log.warn("Generic type not supported")
      null
    }
  }

  private def loadbam(fp: String, region: ReferenceRegion): RDD[T]  = {
    val idxFile: File = new File(fp + ".bai")
    if (!idxFile.exists()) {
      sc.loadBam(fp).filterByOverlappingRegion(region).asInstanceOf[RDD[T]]
    } else {
      sc.loadIndexedBam(fp, region).asInstanceOf[RDD[T]]
    }
  }

  private def loadReference(fp: String, region: ReferenceRegion): RDD[T] = {
    // TODO
    null
  }
  
  def loadFromFile(ks: List[String], region: ReferenceRegion): RDD[(String, T)]  = {
    var ret: RDD[(String, T)] = null
    var load: RDD[(String, T)] = null

    for (k <- ks) {
      if (!fileMap.containsKey(k)) {
        log.error("Key not in FileMap")
        null
      }
      val fp = fileMap(k)
      if (!(new File(fp)).exists()) {
        log.warn("File path for sample " + k + " not loaded")
        null
      }
      if (fp.endsWith(".adam")) {
        load = loadadam(fp, region).map(r => (k, r))
      } else if (fp.endsWith(".sam") || fp.endsWith(".bam")) {
        load = loadbam(fp, region).map(r => (k, r))
      } else if (fp.endsWith(".vcf")) {
        load = sc.loadGenotypes(fp).filterByOverlappingRegion(region).asInstanceOf[RDD[T]].map(r => (k, r))
      } else if (fp.endsWith(".bed")) {
        load = sc.loadFeatures(fp).filterByOverlappingRegion(region).asInstanceOf[RDD[T]].map(r => (k, r))
      } else if(fp.endsWith(".fa") || fp.endsWith(".fasta")) {
        load = loadReference(fp, region).map(r => (k, r))
      } else {
        throw UnsupportedFileException("File type not supported")
      }
      if (ret == null) {
        ret = load
      } else {
        ret = ret.union(load)
      }
    }

    ret
  }

  def get(region: ReferenceRegion, k: String): Option[Map[ReferenceRegion, List[(String, List[T])]]] = multiget(region, List(k))

	/* If the RDD has not been initialized, initialize it to the first get request
	* Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
	* If it exists, call get on the IntervalRDD
	* Otherwise call put on the sections of data that don't exist
	* Here, ks, is an option of list of personids (String)
	*/
	def multiget(region: ReferenceRegion, ks: List[String]): Option[Map[ReferenceRegion, List[(String, List[T])]]] = {

    val matRegion: ReferenceRegion = getChunk(region)
    println("in multiget")
		if (intRDD == null) {
      println("rdd is null")
      // load all data from keys
      val rdd: RDD[(ReferenceRegion, (String, List[T]))] = loadFromFile(ks, matRegion).groupByKey.map(r => (region, (r._1, r._2.toList)))
  		intRDD = IntervalRDD(rdd, dict)
      rememberValues(matRegion, ks)
      filterByRegion(intRDD.multiget(region, Option(ks)))
		} else {
      val regions = partitionChunk(matRegion)
      println("rdd is NOT null")
      for (r <- regions) {
        println(r)
        val keys = bookkeep(r.referenceName).search(r, ks)
        val found: List[String] = keys.map(k => k._1)
        val notFound: List[String] = ks.filterNot(found.contains(_))
        put(r, notFound)
      }
      filterByRegion(intRDD.multiget(region, Option(ks)))
		}
    
	}


	/* Transparent to the user, should only be called by get if IntervalRDD.get does not return data
	* Fetches the data from disk, using predicates and range filtering
	* Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
	*/
  private def put(region: ReferenceRegion, ks: List[String]) = {
    println("keys not in rdd")
    println(ks)
    val rdd: RDD[(ReferenceRegion, (String, List[T]))] = loadFromFile(ks, region).groupByKey.map(r => (region, (r._1, r._2.toList)))
    intRDD.multiput(rdd, dict)
  	rememberValues(region, ks)
    null
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


   private def filterByRegion(map: Option[Map[ReferenceRegion, List[(String, List[T])]]]): Option[Map[ReferenceRegion, List[(String, List[T])]]] = {
    val data = map.get
    data.foreach((rec: (ReferenceRegion, List[(String, List[T])])) => (rec._1, filterRecords(rec._1, rec._2)))
    Option(data)
  }

  private def filterRecords(region: ReferenceRegion, data: List[(String, List[T])]): List[(String, List[T])] = {
    val isAlignmentRecord = classOf[AlignmentRecord].isAssignableFrom(classTag[T].runtimeClass)

    if (isAlignmentRecord) {
      val alignmentData = data.asInstanceOf[List[(String, List[AlignmentRecord])]]
      alignmentData.foreach(rec => (rec._1, rec._2.filter(r => 
        region.overlaps(new ReferenceRegion(r.contig.contigName, r.start, r.end)))))
      alignmentData.asInstanceOf[List[(String, List[T])]]
    } else {
      log.warn("Generic datatype not supported")
      data
    }
  }

}

case class UnsupportedFileException(message: String) extends Exception(message)


//Takes in a file path, contains adapters to pull in RDD[BDGFormat]
object LazyMaterialization {

  //Create a Lazy Materialization object by feeding in a file path to build an RDD from
	def apply[T: ClassTag](sc: SparkContext): LazyMaterialization[T] = {
		new LazyMaterialization[T](sc)
	}
  def apply[T: ClassTag](sc: SparkContext, chunkSize: Long): LazyMaterialization[T] = {
    new LazyMaterialization[T](sc, chunkSize)
  }
}