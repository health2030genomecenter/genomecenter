# Health 2030 Genome Center data processing pipeline 

## How to build 

You need `sbt` to build (`brew install sbt`). Documentation: https://www.scala-sbt.org/1.x/docs/index.html

- Type `compile` to sbt's command prompt. This compiles all sources
- `test` runs all unit tests. This needs the test data folder which is not checked into this repository. `test` compiles all sources if not yet compiled.
- `it:test` runs all integration test. 
- `pipelineExecutor/universal:packageZipTarball` packages up the application in `pipeline-executor/target/universal/*gz`. 

# Tree structure

- `docker/` various Dockerfiles for staging and testing
- `pipeline-executor/` source code of the main application
- `project/` sbt plugins and project definition
- `readqc/` helper application to extract summary statistics from fastq files 
- `readqccli/` cli interface for `readqc`
- `tasks-slum/` library providing slurm support
- `umiprocessor/` helper application to process UMIs
- `fqsplit/` helper application which splits tuples of fastq files into bounded batches of tuples of fastq files
- `cli/` command line interface to the whole application
- `.gitlab-ci.yml` continuous integration job definitions
- `build.sbt` sbt build definition
- `test.sh` runs the tests in a suitable docker container locally
