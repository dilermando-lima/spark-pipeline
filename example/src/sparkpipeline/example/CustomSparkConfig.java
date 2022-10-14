package sparkpipeline.example;

import static org.apache.spark.sql.functions.sum;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;

import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;
import sparkpipeline.core.writer.WriterCSV;

class CustomSparkConfig {

    static final String DATASET_1 = "DATASET_1_NAME";
    static final String DATASET_1_PATH_INPUT = "example/src-resource/fileA.csv";
    static final String DATASET_1_TRANSF_PATH_OUTPUT = "example/build/temp-outputs/LogConfigExample";
    public static void main(String[] args) {

        Map<String,Object> mapVars = new HashMap<>();
        mapVars.put("ENVIRONMENT", "PRD");
        mapVars.put("UI_ENABLE", "true");

        PipelineContextBuilder contextBuilder = PipelineContextBuilder
            .init()
            .collectVarsFromMap(mapVars)
            .sparkConfigBuilder(context -> 
                new SparkConf()
                        .setAppName(context.handleStringFromContextVars("APP-NAME-${ENVIRONMENT}"))
                        .setMaster("local[1]")
                        .set("spark.sql.shuffle.partitions", "1")
                        .set("spark.ui.enabled", context.varByKey("UI_ENABLE",String.class))
            );

        Pipeline.initWithContextBuilder(contextBuilder)
                .read(DATASET_1, ReaderCSV.init(DATASET_1_PATH_INPUT).hasHeader(true))
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())
                .transform(DATASET_1, context -> context.datasetByKey(DATASET_1).groupBy("category").agg(sum("value")))
                .persist(DATASET_1)
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())
                .write(DATASET_1, WriterCSV.init(DATASET_1_TRANSF_PATH_OUTPUT))
                .execute();
    }
}