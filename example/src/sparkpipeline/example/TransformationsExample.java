package sparkpipeline.example;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;

class TransformationsExample {

    static final String DATASET_1 = "DATASET_1_NAME";
    static final String DATASET_2 = "DATASET_2_NAME";
    static final String DATASET_3 = "DATASET_3_NAME";
    static final String DATASET_1_PATH_INPUT = "example/src-resource/fileA.csv";
    static final String DATASET_1_TRANSF_PATH_OUTPUT = "example/build/temp-outputs/TransformationsExample";

    public static void main(String[] args) {
        Pipeline.init()
                .read(DATASET_1, ReaderCSV.init(DATASET_1_PATH_INPUT).hasHeader(true))
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())

                // transform dataset with methods
                .transform(DATASET_1, context -> context.datasetByKey(DATASET_1).filter(col("category").notEqual("A")))
                // transform dataset with sqlContext
                .transformSql(DATASET_1, context -> String.format("select * from %s where category <> 'B'", DATASET_1))
                
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())

                // transform dataset with methods into new dataset
                .transform(DATASET_2, context -> context.datasetByKey(DATASET_1).groupBy("category").agg(sum("value")))
               // transform dataset with sqlContext into new dataset
                .transformSql(DATASET_3, context -> String.format("select * from %s where category is not null", DATASET_1))
                
                .anyRunning(context -> {
                    context.datasetByKey(DATASET_1).show();
                    context.datasetByKey(DATASET_2).show();
                    context.datasetByKey(DATASET_3).show();
                })

                .execute();
    }
}