lazy val commonSettings = Seq(
  scalaVersion := "2.12.7",
  parallelExecution in Test := false,
  scalacOptions ++= scalacOptionList,
  scalacOptions in (Compile, console) := scalacOptions.value filterNot (_ == "-Ywarn-unused:imports")
) ++ Seq(
  organization := "org.gc",
  fork := true,
  javaOptions += "-Xmx4G",
  cancelable in Global := true,
  git.useGitDescribe := true
)

lazy val tasksVersion = "0.0.44"

lazy val stagingIvy1Repository =
  Resolver.url("gc-ivy1",
               new java.net.URL("http://10.6.38.2:31080/artifactory/ivy1"))(
    Resolver.ivyStylePatterns)

lazy val publishToStagingArtifactory = Seq(
  publishTo := Some(stagingIvy1Repository)
)

resolvers += Resolver.jcenterRepo

lazy val tasksSlurm = project
  .in(file("tasks-slurm"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-slurm",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "tasks-core" % tasksVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.18" % "test"
    )
  )
  .settings(publishToStagingArtifactory: _*)

lazy val readqc = project
  .in(file("readqc"))
  .settings(commonSettings: _*)
  .settings(
    name := "readqc",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.github.samtools" % "htsjdk" % "2.16.1",
      "io.circe" %% "circe-core" % "0.10.1",
      "io.circe" %% "circe-generic" % "0.10.1",
      "io.github.pityka" %% "fileutils" % "1.2.2" % "test"
    ),
    assemblyJarName := "readqc",
    test in assembly := {},
    unmanagedClasspath in Test += {
      val testFolder = System.getenv("GC_TESTFOLDER")
      if (testFolder == null) baseDirectory.value
      else new File(testFolder).getCanonicalFile
    }
  )

lazy val fqsplit = project
  .in(file("fqsplit"))
  .settings(commonSettings: _*)
  .settings(
    name := "fqsplit",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.github.samtools" % "htsjdk" % "2.16.1",
      "io.github.pityka" %% "fileutils" % "1.2.2",
      "com.intel.gkl" % "gkl" % "0.8.5" exclude ("com.github.samtools", "htsjdk")
    ),
    assemblyJarName := "fqsplit",
    test in assembly := {},
    unmanagedClasspath in Test += {
      val testFolder = System.getenv("GC_TESTFOLDER")
      if (testFolder == null) baseDirectory.value
      else new File(testFolder).getCanonicalFile
    }
  )

lazy val readqcCLI = project
  .in(file("readqccli"))
  .settings(commonSettings: _*)
  .settings(
    name := "readqccli",
    assemblyJarName := "readqc",
    test in assembly := {}
  )
  .dependsOn(readqc)

lazy val umiProcessor = project
  .in(file("umiprocessor"))
  .settings(commonSettings: _*)
  .settings(
    name := "umiprocessor",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.github.samtools" % "htsjdk" % "2.16.1",
      "io.github.pityka" %% "fileutils" % "1.2.2" % "test"
    ),
    assemblyJarName := "umiprocessor",
    test in assembly := {}
  )

lazy val pipelineExecutor = project
  .in(file("pipeline-executor"))
  .settings(commonSettings: _*)
  .settings(
    name := "pipeline-executor",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "tasks-core" % tasksVersion,
      "io.github.pityka" %% "tasks-collection" % tasksVersion,
      "io.github.pityka" %% "tasks-ui-backend" % tasksVersion,
      "io.github.pityka" %% "tasks-tracker" % tasksVersion,
      "io.github.pityka" %% "fileutils" % "1.2.2",
      "io.github.pityka" %% "nspl-core" % "0.0.20",
      "io.github.pityka" %% "nspl-awt" % "0.0.20",
      "io.github.pityka" %% "intervaltree" % "1.0.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % "it,test",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.18",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.18" % "it,test",
      "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.18" % "it,test",
      "com.typesafe.akka" %% "akka-http-testkit" % "10.1.5" % "it,test" exclude ("com.typesafe.akka", "akka-stream-testkit_2.12"),
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.20",
      "de.heikoseeberger" %% "akka-http-circe" % "1.22.0",
      "com.github.samtools" % "htsjdk" % "2.16.1",
      "com.github.pathikrit" %% "better-files" % "3.6.0",
      "io.circe" %% "circe-core" % "0.10.1",
      "io.circe" %% "circe-generic" % "0.10.1",
      "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
      "com.github.scopt" %% "scopt" % "4.0.0-RC2",
      "org.scalaj" %% "scalaj-http" % "2.4.1",
      "com.hierynomus" % "sshj" % "0.27.0",
      "com.intel.gkl" % "gkl" % "0.8.5" exclude ("com.github.samtools", "htsjdk")
    ),
    unmanagedClasspath in Test += {
      val testFolder = System.getenv("GC_TESTFOLDER")
      if (testFolder == null) baseDirectory.value
      else new File(testFolder).getCanonicalFile
    }
  )
  .dependsOn(tasksSlurm, readqc, fqsplit)
  .enablePlugins(JavaServerAppPackaging)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    // To have access to the build version during runtime
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "org.gc.buildinfo"
  )
  .settings(
    // This amends the launch script to put ../resources onto the classpath
    scriptClasspath := scriptClasspath.value :+ "../resources/"
  )
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings
  )
  .settings(
    // This makes the fat jar of the umiProcessor project available on the classpath
    // first at normal deployment with universal
    // second at test
    // (resources in Compile) would put the jar into a jar which slows the buidl.
    mappings in Universal += (assembly in Compile in umiProcessor).value -> "resources/umiprocessor",
    resources in Test += (assembly in Compile in umiProcessor).value,
    resources in IntegrationTest += (assembly in Compile in umiProcessor).value,
    mappings in Universal += (assembly in Compile in readqcCLI).value -> "resources/readqc",
    resources in Test += (assembly in Compile in readqcCLI).value,
    resources in IntegrationTest += (assembly in Compile in readqcCLI).value,
    mappings in Universal += (assembly in Compile in fqsplit).value -> "resources/fqsplit",
    resources in Test += (assembly in Compile in fqsplit).value,
    resources in IntegrationTest += (assembly in Compile in fqsplit).value
  )
  .settings(
    artifactName in packageSrc in Compile := {
      (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
        "sources.jar"
    },
    resources in IntegrationTest += (packageSrc in Compile).value,
    mappings in Universal += (packageSrc in Compile).value -> "resources/sources.jar"
  )
  .settings(unmanagedClasspath in IntegrationTest += {
    val testFolder = System.getenv("GC_TESTFOLDER")
    if (testFolder == null) baseDirectory.value
    else new File(testFolder).getCanonicalFile
  })
  .settings(inConfig(IntegrationTest)(scalafmtSettings))
  .settings(
    publishToStagingArtifactory: _*
  )

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false
  )
  .aggregate(tasksSlurm,
             pipelineExecutor,
             umiProcessor,
             readqc,
             readqcCLI,
             fqsplit)
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
