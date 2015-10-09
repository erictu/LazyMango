import sbt._
object intervalrdd extends Build
{
  lazy val root =
    Project("root", file(".")) dependsOn(unfiltered, interval)
  lazy val interval =
  	RootProject(uri("../spark-intervalrdd"))
  lazy val unfiltered =
    RootProject(uri("../interval-tree"))
}