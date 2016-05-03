name := "SSBQueries"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"

resolvers += Resolver.sonatypeRepo("public")
