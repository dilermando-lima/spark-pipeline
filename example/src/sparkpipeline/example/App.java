package sparkpipeline.example;

import org.apache.logging.log4j.Level;
import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.log.LogConfig;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;
import sparkpipeline.core.writer.WriterCSV;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

class App {

    public static void main(String[] args) {

        LogConfig logConfig = LogConfig.init()
                // .logConsolePattern("%d{HH:mm:ss.sss} %p %25.25c : %m%n")
                .logConsoleLevel("org.apache.spark", Level.WARN)
                .logConsoleLevel("sparkpipeline.core", Level.INFO);

        PipelineContextBuilder contextBuilder = PipelineContextBuilder
                .init()
                .setLogConfig(logConfig)
                .collectVarsFromArgs(args)
                .collectVarsFromMap(
                        new HashMap<String, Object>() {
                            {
                                put("ROOT_PATH", "/home/dilermando.lima/Downloads/test-spark-pp");
                                // put("INPUT_PATH","${ROOT_PATH}/input-test.txt");
                                put("OUT_PATH_1", "${ROOT_PATH}/dataset1.csv");
                                put("OUT_PATH_2", "${ROOT_PATH}/dataset2.csv");
                            }
                        })
                .collectVarsFromProperties("${ROOT_PATH}/config.env")
                .afterRetrieveAllContextVars(context -> {
                    context.newVar("TESTE", "RER");
                });

        Pipeline.initWithContextBuilder(contextBuilder)
                .read("dataset1", ReaderCSV.init("${INPUT_PATH}").hasHeader(true))
                .persist("dataset1")
                .read("dataset2", ReaderCSV.init("${INPUT_PATH}").hasHeader(true))
                .persist("dataset2")
                .anyRunning(context -> context.datasetByKey("dataset1").show())
                .anyRunning(context -> context.datasetByKey("dataset2").show())
                .anyRunning(context -> {
                    context.newVar("VAR_CODE_1", "TESTE");
                    context.newVar("VAR_CODE_2", 4);
                    context.newVar("VAR_CODE_3", new ArrayList<>(Arrays.asList("tet", "saesfd")));
                    context.newVar("VAR_CODE_4", new HashMap<String, String>() {
                        {
                            put("key1", "value1");
                        }
                    });
                })
                .transform("dataset1",
                        context -> context.datasetByKey("dataset1").groupBy("type", "number").agg(sum("number")))
                .transformSql("dataset2", context -> "select * from dataset2 where type = 'b'")
                .validate(context -> {
                    if (context.datasetByKey("dataset2").isEmpty())
                        throw new RuntimeException("dataset2 is empty");
                })
                .anyRunning(context -> context.datasetByKey("dataset1").show())
                .anyRunning(context -> context.datasetByKey("dataset2").show())
                .anyRunning(context -> {
                    Object a = context.varByKey("VAR_CODE_1");
                    Object b = context.varByKey("VAR_CODE_1", String.class);
                    String c = (String) context.varByKey("VAR_CODE_1");
                    Object d = context.varByKey("VAR_CODE_2");
                    Object e = context.varByKey("VAR_CODE_2", Integer.class);
                    List f = context.varByKey("VAR_CODE_3", List.class);
                    List<String> g = (List<String>) context.varByKey("VAR_CODE_3");
                    Map h = context.varByKey("VAR_CODE_4", Map.class);
                    Map<String, Long> i = (Map<String, Long>) context.varByKey("VAR_CODE_4");

                })
                .write("dataset1", WriterCSV.init("${OUT_PATH_1}"))
                .write("dataset2", WriterCSV.init("${OUT_PATH_2}"))
                .unpersist("dataset1", "dataset2")
                .execute();

        // new Mng()
        // .insert(() -> "insert1")
        // .modify(String::toUpperCase)
        // .modify(String::toLowerCase)
        // .insert(() -> "insert2")
        // .modify(name -> name.substring(2,4))
        // .modify(String::toUpperCase)
        // .exec();

        //
        // Pipeline pipeline1 = Pipeline.init()
        // //.overrideContext(PipelineContext.init().newPath("path1",Path.of("/home/dilermando.lima/Downloads/input-test.txt")))
        // .read("dataset1",ReaderCSV.init().pathKeyInput("path1"))
        // .anyRunning(context -> context.datasetByKey("dataset1").show())
        // .transform("dataset1",(context, dataset) -> dataset.coalesce(1))
        // .transform("dataset1", (context, dataset) -> dataset.groupBy("type",
        // "number").agg(sum("number")))
        // .anyRunning(context -> context.datasetByKey("dataset1").show())
        // .write(WriterCSV.init().mockEnable(context -> true).mockWriter(context ->
        // System.out.println("")));
        //
        //
        //
        // Pipeline pipeline2 = Pipeline.init()
        // //.overrideContext(PipelineContext.init().newPath("path1",Path.of("/home/dilermando.lima/Downloads/input-test.txt")))
        // .read("dataset1",ReaderCSV.init().pathKeyInput("path1"))
        // .anyRunning(context -> context.datasetByKey("dataset1").show())
        // .transform("dataset1",(context, dataset) -> dataset.coalesce(1))
        // .transformToNewOne("dataset1", (context, dataset) -> dataset.groupBy("type",
        // "number").agg(sum("number")))
        // .anyRunning(context -> context.datasetByKey("dataset1").show())
        // .write(WriterCSV.init().mockEnable(context -> true).mockWriter(context ->
        // System.out.println("")));
        //
        //
        // StrategyPipeline
        // .input(6)
        // .overrideContext(PipelineContext.init().newPath("path1",Path.of("/home/dilermando.lima/Downloads/input-test.txt")))
        // .addPipeline(pipeline1,num -> num == 6)
        // .addPipeline(pipeline1,num -> num > 6)
        // .addPipeline(pipeline2,num -> num == 6)
        // .exec();
        //
        //
        //
        //

        // var pipeline =
        // Pipeline.init().read().read().transform().cacheAdd().run().write();

        // String keyFile1 = "path-file-1";
        // PipelineContext context = new PipelineContext();
        // context.newPath(keyFile1, pipelineContext ->
        // "/home/dilermando.lima/Downloads/input-test.txt");

        // context.sparkSession().udf().register(
        // "addPrefix",
        // stringObjectUDF1,
        // DataTypes.StringType
        // );

        // dataset.createOrReplaceTempView("dtset");
        //
        // dataset.sqlContext().sql("select addPrefix(type) as type_changed, nam2e,
        // number from dtset where nam2e = 'name1'").show();
        // //.as(Encoders.javaSerialization(InputTestInt.class))
        //
        //



    }
}