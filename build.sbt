import sbt.Keys.{licenses, _}
import sbt.Package.ManifestAttributes

import java.util.{Date, TimeZone}
import sbt._

import java.text.SimpleDateFormat

val scala212 = "2.12.12"
val supportedScalaVersions = List(scala212)

val MAJOR = "0"
val MINOR = "0"
val BUILD_NUMBER = "1"

ThisBuild / scalaVersion := scala212
ThisBuild / organization := "com.dgs"
ThisBuild / version := s"$MAJOR.$MINOR.$BUILD_NUMBER"
ThisBuild / resolvers ++= Seq(Resolver.defaultLocal)
ThisBuild / Test / fork := true
ThisBuild / scalacOptions +="-Ypartial-unification"

ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixOnCompile := false

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
lazy val sparkCore = "org.apache.spark" %% "spark-core" % "3.0.1"
lazy val sparkMLLib = "org.apache.spark" %% "spark-mllib" % "3.0.1"

lazy val catsLib = "org.typelevel" %% "cats-core" % "2.1.1"

lazy val buildTimestamp: String = {
  val currentTime = System.currentTimeMillis()
  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ZZZZ")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
  sdf.format(new Date(currentTime))
}

lazy val commonSettings = Seq(
  homepage := Some(url("https://github.com/datagridsoftware/datamassage")),
  scmInfo := Some(ScmInfo(url("https://github.com/datagridsoftware/datamassage"), "git@github.com:datagridsoftware/datamassage.git")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  crossScalaVersions := supportedScalaVersions,

  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
  publishM2Configuration := publishM2Configuration.value.withOverwrite(true),
  pomIncludeRepository := { (repo: MavenRepository) => false },
  packageOptions := Seq(ManifestAttributes(("Build-Time", buildTimestamp))),
  organizationName := "DataGridSoftware",
  startYear := Some(2021),
  licenses += ("Apache-2.0") -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")

)

lazy val scalaLibrary = (project in file("library/scala"))
  .settings(commonSettings: _*)
  .settings(
    name := "autodatamassage",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkCore,
    libraryDependencies += sparkMLLib,
    libraryDependencies += catsLib
  )

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(name := "datamassage")
  .aggregate(scalaLibrary)

