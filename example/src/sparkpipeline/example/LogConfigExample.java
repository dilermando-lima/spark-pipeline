package sparkpipeline.example;

import static org.apache.spark.sql.functions.sum;

import org.apache.logging.log4j.Level;

import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.log.LogConfig;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;
import sparkpipeline.core.writer.WriterCSV;

class LogConfigExample {

    static final String DATASET_1 = "DATASET_1_NAME";
    static final String DATASET_1_PATH_INPUT = "example/src-resource/fileA.csv";
    static final String DATASET_1_TRANSF_PATH_OUTPUT = "example/build/temp-outputs/LogConfigExample";

    public static void main(String[] args) {

        LogConfig logConfig = LogConfig.init()
                // .logConsolePattern("%d{HH:mm:ss.sss} %p %25.25c : %m%n")
                .logConsoleLevel("org.apache.spark", Level.WARN) // silent spark logs
                .logConsoleLevel("sparkpipeline.core", Level.INFO)
                .logConsoleLevel("sparkpipeline.example", Level.INFO);

        PipelineContextBuilder contextBuilder = PipelineContextBuilder
                .init()
                .setLogConfig(logConfig);

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