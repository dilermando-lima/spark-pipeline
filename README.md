# SPARK-PIPELINE

  * [About this project](#about-this-project)
  * [Simple example using spark-pipeline](#simple-example-using-spark-pipeline)
  * [Implement dependency in your project](#implement-dependency-in-your-project)
    + [Download from git](#download-from-git)
    + [Add  as sourceControl and use as multimodule](#add--as-sourcecontrol-and-use-as-multimodule)
  * [Requirements](#requirements)
  * [Context Builder](#context-builder)
  * [Code Examples](#code-examples)
    + [Customizing logs programatically](#customizing-logs-programatically)
    + [Handle arguments, variables and external configurations](#handle-arguments--variables-and-external-configurations)

## About this project
spark-pipeline project is a solutions to solve all chalenges belows when we are working with [apache spark](https://spark.apache.org/docs/latest/index.html) in huge projects and complex solutions.

  - Create pipeline flows declared by step managing all executions
  - Manage input variables by application argumments, environment variables, getting from files and other implementations
  - Manage dataset reading, transformation and loading through pipelines
  - Manage log configuration programatically for all application, this project and even apache spark
  - Give us a way to mock reading and loading actions by condictions as you prefer

## Simple example using spark-pipeline

Simple example using pipeline
```Java
Pipeline
.init() // spark session will be created as default config
.read("DATASET_1", ReaderCSV.init("${DATASET_1_PATH_INPUT}").hasHeader(true))
.anyRunning(context -> context.datasetByKey("DATASET_1").show())
.transform("DATASET_1", context -> context.datasetByKey("DATASET_1").filter(...))
.transformSql("DATASET_1", context -> "select * from `DATASET_1` where ...")
.anyRunning(context -> context.datasetByKey("DATASET_1").show())
.transform("DATASET_2", context -> context.datasetByKey("DATASET_1").groupBy(...)))
.persist("DATASET_1")
.transformSql("DATASET_3", context -> "select * from `DATASET_1` where ...")
.write(("DATASET_3", WriterCSV.init("${DATASET_1_TRANSF_PATH_OUTPUT}"))
.execute(); // all steps are functional callings and will be run only right here
```

## Implement dependency in your project

All core source are place in [./core/src/*](./core/src/sparkpipeline/core)

### Download from git

```Shell
git clone https://github.com/dilermando-lima/spark-pipeline.git
```


### Add  as sourceControl and use as multimodule

add into `setting.gradle` file
```Groovy
sourceControl {
  gitRepository("https://github.com/dilermando-lima/spark-pipeline.git") {
    producesModule("sparkpipeline.core:core")
  }
}
```

add into `build.gradle` dependencies
```Groovy
dependencies {
  implementation('sparkpipeline.core:core') {
      version {
          branch = 'main'
      }
  }
}

```
> Required Gradle 4.x to use sourceControl example above in your project

## Requirements

Your project must contains:
  - Version source java8+
  - Spark core dependencie
  - Spark sql dependencie
  
exampĺe at `build.gradle`

```Groovy
sourceCompatibility=1.8 // java8+
targetCompatibility=1.8 // java8+

dependencies {
    implementation(project(":core"))
    // Spark core dependencie
    implementation 'org.apache.spark:spark-core_2.13:3.3.0'
    // Spark sql dependencie
    implementation 'org.apache.spark:spark-sql_2.13:3.3.0'
}
```
> All example source are place in [./example/src/*](./example/src/sparkpipeline/example)

## Context Builder

When creating a pipeline with `.init()` will be used default settings
```Java
Pipeline.init().anyRunning(...).execute();
```
If you need add custom setting into context on pipeline executions use `PipelineContextBuilder`

```Java
// declare contextBuilder
PipelineContextBuilder contextBuilder = PipelineContextBuilder
    .init()

    // collect vars into context
    .collectVarsFromArgs(...)
    .collectVarsFromFile(...)
    .collectVarsFromMap(...)
    .addVarCollector(...) // custom implementation

    // log settings
    .setLogConfig(...)

    // settings to limit rerun steps and pipeline
    .setMaxAmountReRunEachStep(...)
    .setMaxAmountReRunPipeline(...)

    // add custom spark config configuration
    .sparkConfigBuilder(...)
    
    // add actions into start-cycle context
    .beforeStartContext(...)
    .afterStartLogConfig(...)
    .afterStartContext(...)
    .afterRetrieveAllContextVars(...); 

// creating pipeline with contextBuilder created above
Pipeline.initWithContextBuilder(contextBuilder)
    .anyRunning(context -> { 
      /* running any peace of code with custom settings in context  */
    })
    .execute();

```
## Code Examples

### Customizing logs programatically

```Java

LogConfig logConfig = LogConfig.init()
  // .logConsolePattern("%d{HH:mm:ss.sss} %p %25.25c : %m%n")
  .logConsoleLevel("org.apache.spark", Level.WARN) // silent spark logs
  .logConsoleLevel("sparkpipeline.core", Level.INFO)
  .logConsoleLevel("com.my.application", Level.INFO);

PipelineContextBuilder contextBuilder = PipelineContextBuilder
  .init()
  .setLogConfig(logConfig);

Pipeline
  .initWithContextBuilder(contextBuilder)
  // all step declaration here...
  .execute();

```

> Complete example for log settngs in [./example/src/sparkpipeline/example/LogConfigExample.java*](./example/src/sparkpipeline/example/LogConfigExample.java)

### Handle arguments, variables and external configurations

`spark-pipeline` has by default 3 implementations to collect variables into context:
 - by Arguments
 - from Map<String,Object>
 - file reading ( file will be reading by sparkSession )

We can use variables declaring as `${VAR}` or `${VAR:default_value}`

```Java
PipelineContextBuilder contextBuilder = PipelineContextBuilder
	.init()
	.collectVarsFromArgs(args) // collect vars from application vars
	.collectVarsFromMap(Map.of("ENVIRONMENT", "prd")) // collect from map
	.collectVarsFromFile("vars1.env") // collect from file
	.collectVarsFromFile("vars2-${ENVIRONMENT}.properties") // collect from file using var already added before
	.addVarCollector(new MyCollectorVars()); // collect using customized implementation

Pipeline.initWithContextBuilder(contextBuilder)
	.anyRunning(context -> {
		System.out.println("ENVIRONMENT: " + context.varByKey("ENVIRONMENT"));
	})
	.read("DATASET1", ReaderCSV.init("${DATASET_1_PATH_INPUT}")) // get path from context vars
	.write(DATASET_1, WriterCSV.init("${DATASET_1_TRANSF_PATH_OUTPUT}")) // get path from context vars
	.execute();
```

> Complete example for log settngs in [./example/src/sparkpipeline/example/EnvVarsExample.java*](./example/src/sparkpipeline/example/EnvVarsExample.java)

