name := "LazyMango"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

publishMavenStyle := true

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.0" % "test"
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli" % "0.18.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.0"
libraryDependencies += "org.bdgenomics.utils" %% "utils-misc" % "0.2.3"

//libraryDependencies += "org.apache.parquet" %% "parquet-avro" % "1.8.1"
libraryDependencies += "org.apache.parquet" %% "parquet-scala" % "1.8.1"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

javaOptions in test += "-Xmx2G"

fork in test := true