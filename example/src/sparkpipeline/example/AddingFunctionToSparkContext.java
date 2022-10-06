package sparkpipeline.example;

import org.apache.spark.sql.types.DataTypes;

import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;

class AddingFunctionToSparkContext {
    
    static final String DATASET_1_PATH_INPUT = "example/src-resource/fileA.csv";

    @SuppressWarnings("java:S1192")
    public static void main(String[] args) {

        PipelineContextBuilder contextBuilder = PipelineContextBuilder
        .init()
        .afterStartContext(context -> {
            context.sparkSession().udf().register(
                "addPrefix",
                (String prefix,String text) -> prefix + text ,
                DataTypes.StringType
            );
            context.sparkSession().udf().register(
                "addSufix",
                (String sufix,String text) -> text + sufix ,
                DataTypes.StringType
            );
        });


        Pipeline.initWithContextBuilder(contextBuilder)
                .read("DATASET_1", ReaderCSV.init(DATASET_1_PATH_INPUT).hasHeader(true)) // placed in ./example/src-resource/fileA.csv
                .anyRunning(context -> context.datasetByKey("DATASET_1").show())
                // transform dataset using custom functions
                .transformSql("DATASET_1", context -> 
                    "select addPrefix('CATEG-',category) , addSufix('-NAME',name), value from DATASET_1"
                )
                .anyRunning(context -> context.datasetByKey("DATASET_1").show())
                .execute();
    }
}