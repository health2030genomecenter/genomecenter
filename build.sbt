lazy val commonSettings = Seq(
  scalaVersion := "2.12.6",
  parallelExecution in Test := false,
  scalacOptions ++= scalacOptionList,
  scalacOptions in (Compile, console) := scalacOptions.value filterNot (_ == "-Ywarn-unused:imports")
) ++ Seq(
  organization := "org.gc",
  fork := true,
  cancelable in Global := true,
  git.useGitDescribe := true
)

resolvers += Resolver.jcenterRepo

lazy val tasksSlurm = project
  .in(file("tasks-slurm"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-slurm",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "tasks-core" % "0.0.21-SNAPSHOT",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.16" % "test"
    )
  )

lazy val pipelineExecutor = project
  .in(file("pipeline-executor"))
  .settings(commonSettings: _*)
  .settings(
    name := "pipeline-executor",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "tasks-core" % "0.0.21-SNAPSHOT",
      "io.github.pityka" %% "fileutils" % "1.2.2",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.16",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.16" % "test",
      "com.typesafe.akka" %% "akka-http-testkit" % "10.1.5" % "test",
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.20",
      "de.heikoseeberger" %% "akka-http-circe" % "1.22.0",
      "com.github.samtools" % "htsjdk" % "2.16.1"
    ),
    unmanagedClasspath in Test += {
      val testFolder = System.getenv("GC_TESTFOLDER")
      if (testFolder == null) baseDirectory.value
      else new File(testFolder).getCanonicalFile
    }
  )
  .dependsOn(tasksSlurm)
  .enablePlugins(JavaServerAppPackaging)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "org.gc.buildinfo"
  )
  .settings(
    scriptClasspath := scriptClasspath.value :+ "../resources/"
  )

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false
  )
  .aggregate(tasksSlurm, pipelineExecutor)
  .enablePlugins(GitVersioning)

scalafmtOnCompile in ThisBuild := true

parallelExecution in ThisBuild := false

lazy val scalacOptionList = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:postfixOps",
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
  "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match", // Pattern match may not be typesafe.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals", // Warn if a local definition is unused.
  "-Ywarn-unused:params", // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates" // Warn if a private member is unused.
)
