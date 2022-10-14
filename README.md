# SPARK-PIPELINE

  * [About this project](#about-this-project)
  * [Simple example using spark-pipeline](#simple-example-using-spark-pipeline)
  * [Implement dependency in your project](#implement-dependency-in-your-project)
    + [Download from git](#download-from-git)
    + [Add  as sourceControl and use as multimodule](#add-as-sourcecontrol-and-use-as-multimodule)
  * [Requirements](#requirements)
  * [Context Builder](#context-builder)
  * [Code Examples](#code-examples)
    + [Customizing logs programmatically](#customizing-logs-programmatically)
    + [Handle arguments, variables and external configurations](#handle-arguments-variables-and-external-configurations)
    + [Add and retrieve vars into context](#add-and-retrieve-vars-into-context)
    + [Customizing spark configurations](#customizing-spark-configurations)
    + [Transform datasets in pipeline steps](#transform-datasets-in-pipeline-steps)
    + [Read and writing datasets](#read-and-writing-datasets)
    + [Mocki reading and writing in steps](#mocki-reading-and-writing-in-steps)
    + [Manage flow of running in pipeline steps](#manage-flow-of-running-in-pipeline-steps)
    + [Adding Custom functions to spartk session](#adding-custom-functions-to-spartk-session)

## About this project
spark-pipeline project is a solutions to solve all challenges below when we are working with [apache spark](https://spark.apache.org/docs/latest/index.html) in huge projects and complex solutions.

  - Create pipeline flows declared by step managing all executions
  - Manage input variables by application argumments, environment variables, getting from files and other implementations
  - Manage dataset reading, transformation and loading through pipelines
  - Manage log configuration programmatically for all application, this project and even apache spark
  - Give us a way to mock reading and loading actions by conditions as you prefer

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

All core source is place in [./core/src/*](./core/src/sparkpipeline/core)

### Download from git

```Shell
git clone https://github.com/dilermando-lima/spark-pipeline.git
```


### Add as sourceControl and use as multimodule

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

Your project must contain:
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
    
    // add actions into start-cycle context as folow in the next order
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

### Customizing logs programmatically

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

> Complete example for log settngs in [./example/src/sparkpipeline/example/LogConfigExample.java](./example/src/sparkpipeline/example/LogConfigExample.java)

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

> Complete example for args and env settings placed in [./example/src/sparkpipeline/example/EnvVarsExample.java](./example/src/sparkpipeline/example/EnvVarsExample.java)


### Add and retrieve vars into context

```Java
Pipeline.init()
	// saving var into context
	.anyRunning(context -> {
		context.newVar("VAR_1", "value_var_1");
	})
	// retrieving vars from context
	.anyRunning(context -> {
		String var1AsString =  context.varByKey("VAR_1",String.class);
		String var1WithCast =  (String) context.varByKey("VAR_1");
		String varUsingVarDeclarations = context.handleStringFromContextVars("var1 = ${VAR_1}, var-not-found ${VAR_NOT_FOUND:default_value}");
	})
	.execute();
```

> Complete example for saving vars into context placed in [./example/src/sparkpipeline/example/SavingVarsIntoContext.java](./example/src/sparkpipeline/example/SavingVarsIntoContext.java)

### Customizing spark configurations

```Java
PipelineContextBuilder contextBuilder = PipelineContextBuilder
	.init()
	// set vars be used in .sparkConfigBuilder(...)
	.collectVarsFromArgs(args)  
	// set sparkConfigBuilder
	.sparkConfigBuilder(context -> 
		new SparkConf()
			.setAppName(context.handleStringFromContextVars("APP-NAME-${ENVIRONMENT}"))
			.setMaster(context.varByKey("MARTER_HOST",String.class))
			.set("any-config","")
	);

Pipeline.initWithContextBuilder(contextBuilder)
        // all step declaration here...
        .execute();

```
> Complete example for Customizing spark configurations placed in [./example/src/sparkpipeline/example/CustomSparkConfig.java](./example/src/sparkpipeline/example/CustomSparkConfig.java)


### Transform datasets in pipeline steps

```Java
Pipeline.init()
.read(DATASET_1, ReaderCSV.init(DATASET_1_PATH_INPUT))
// transform dataset with methods
.transform(DATASET_1, context -> context.datasetByKey(DATASET_1).filter(col("category").notEqual("A")))
// transform dataset with sqlContext
.transformSql(DATASET_1, context -> String.format("select * from %s where category <> 'B'", DATASET_1))
// transform dataset with methods into new dataset
.transform(DATASET_2, context -> context.datasetByKey(DATASET_1).groupBy("category").agg(sum("value")))
// transform dataset with sqlContext into new dataset
.transformSql(DATASET_3, context -> String.format("select * from %s where category is not null", DATASET_1))
.execute();
```
We can persti, unpersist and remove datasets from context as follows:

```Java
Pipeline.init()
.read(      "dataset1", ...)
.read(      "dataset2", ...)
.persist(   "dataset1", "dataset2")
.unpersist( "dataset1", "dataset2")
.remove(    "dataset1", "dataset2")
// all next steps
.execute();
```

If you need use specifics methods in dataset on context, use `context.datasetByKey("datasetKey")` inside any step

> Complete example for transformations placed in [./example/src/sparkpipeline/example/TransformationsExample.java](./example/src/sparkpipeline/example/TransformationsExample.java)

### Read and writing datasets

We read and write dataset using implementations of `sparkpipeline.core.reader.AbstractReader` and `sparkpipeline.core.reader.AbstractWriter`

```Java
Pipeline.init()
.read("DATASET_KEY_READ_NEW_DATASET",  sparkpipeline.core.reader.AbstractReader() )
.write("DATASET_KEY_TO_WRITE",  sparkpipeline.core.reader.AbstractWriter() )
.execute();
```

There are 5 reading implementations as default until now:

```Java
sparkpipeline.core.reader.ReaderJDBC.init()...
sparkpipeline.core.reader.ReaderCSV.init()...
sparkpipeline.core.reader.ReaderJSON.init()...
sparkpipeline.core.reader.ReaderText.init()...
sparkpipeline.core.reader.ReaderParquet.init()...
```

There are 2 writing implementations as default until now:

```Java
sparkpipeline.core.reader.WriterCSV.init()...
sparkpipeline.core.reader.WriterParquet.init()...
```

> When using `Pipeline.read()` and `Pipeline.write()` we got all benefits for context vars, mock executions and functional exectution in pipeline steps. 

### Mock reading and writing in steps

All implementations of `sparkpipeline.core.reader.AbstractReader` and `sparkpipeline.core.reader.AbstractWriter` has a way to mock their actions

```Java
WriterCSV reader ReaderCSV.init("${PATH_READ}")
  .mockEnable(context -> context.varByKey("MOCK_DATASET",Boolean.class))
  .mockReader(context -> createMockDataset(context));

WriterCSV writer = WriterCSV.init("${PATH_WRITE}")
	.mockEnable(context -> context.varByKey("MOCK_DATASET",Boolean.class))
	.mockWriter(context -> System.out.println("Pretending writing dataset"));

Pipeline.init()
	.anyRunning(context -> context.newVar("MOCK_DATASET", true)) // set mock on
	.read("DATASET_1", reader) // using reader with mock
	.write("DATASET_1", writer)  // using writer with mock
	.execute();
```

> Complete example for mocks placed in [./example/src/sparkpipeline/example/MockExample.java](./example/src/sparkpipeline/example/MockExample.java)


### Manage flow of running in pipeline steps

if you have needs to rerun steps, abort pipeline or restart pipeline execuction... you can manage step flows from `context.controllerExecution()`in any step

```Java
PipelineContextBuilder contextBuilder = PipelineContextBuilder.init()
	.setMaxAmountReRunEachStep(3) // limit max rerun for steps
	.setMaxAmountReRunPipeline(2);  // limit max rerun for whole pipeline

Pipeline.initWithContextBuilder(contextBuilder)
	// rerun current step
	.anyRunning(context -> {
		context.controllerExecution().reRunCurrentStep();
	})
	// rerun all pipeline
	.anyRunning(context -> {
		context.controllerExecution().reRunAllPipeline();
	})
  	// rerun current step when throw reading
	.read("DATASET_1", ReaderCSV.init("any-path-not-found").whenThrowError(
		(error ,context) -> {
			System.err.println(error.getLocalizedMessage());
			context.controllerExecution().reRunCurrentStep();
		}
	))
	// rerun current step when throw wrinting
	.write("DATASET_1", WriterCSV.init("any-path-not-found").whenThrowError(
		(error ,context) -> {
			System.err.println(error.getLocalizedMessage());
			context.controllerExecution().reRunCurrentStep();
		}
	))
  // abort all pipeline
	.anyRunning(context -> {
		context.controllerExecution().abortPipeline();
	})
	.execute();

```

> Complete example for flow management placed in [./example/src/sparkpipeline/example/ManageStepFlow.java](./example/src/sparkpipeline/example/ManageStepFlow.java)


### Adding Custom functions to spartk session

You can prepare `pipelineContext` in `PipelineContextBuilder` before declare all steps:

```Java
PipelineContextBuilder contextBuilder = PipelineContextBuilder
	.init()
	.afterStartContext(context -> {
		context.sparkSession().udf().register(
			"addSufix",
			(String sufix,String text) -> text + sufix ,
			DataTypes.StringType
		);
	});

Pipeline.initWithContextBuilder(contextBuilder)
	.read("DATASET_1", ReaderCSV.init(DATASET_1_PATH_INPUT))
	// transform dataset using custom functions
	.transformSql("DATASET_1", context -> "select addSufix('-SUFIX', name) from DATASET_1")
	.anyRunning(context -> context.datasetByKey("DATASET_1").show())
	.execute();

```

> If you need use variables from context use `afterRetrieveAllContextVars()`  instead of `afterStartContext()`

> Complete example for adding Custom functions placed in [./example/src/sparkpipeline/example/AddingFunctionToSparkContext.java](./example/src/sparkpipeline/example/AddingFunctionToSparkContext.java)















