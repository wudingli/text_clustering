name := "text_clustering"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.jsoup" % "jsoup" % "1.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"