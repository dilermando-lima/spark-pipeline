package sparkpipeline.example;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import sparkpipeline.core.context.PipelineContext;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderJDBC;
import sparkpipeline.core.writer.WriterCSV;

class MockExample {

    @SuppressWarnings({"java:S1192","java:S106"})
    public static void main(String[] args) {

        ReaderJDBC reader = ReaderJDBC.init()
            .driver("${DRIVER}")
            .url("${URL}")
            .password("${PASS}")
            .user("${USER}")
            .mockEnable(context -> Boolean.TRUE.equals(context.varByKey("MOCK_DATASET_1",Boolean.class)))
            .mockReader(MockExample::createMockDataset);

        WriterCSV writer = WriterCSV.init("${PATH}")
            .mockEnable(context -> context.varByKey("MOCK_DATASET_1",Boolean.class))
            .mockWriter(context -> System.out.println("\n ======== \n Pretending writing dataset-1\n ========= \n"));

        Pipeline.init()
                .anyRunning(context -> context.newVar("MOCK_DATASET_1", true)) // set mock on
                .read("DATASET_1", reader) // using reader with mock
                .anyRunning(context -> context.datasetByKey("DATASET_1").show())
                .write("DATASET_1", writer)  // using writer with mock
                .execute();
    }


    private static java.sql.Date stringToSqlDate(String stringDate){
        try {
            return new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(stringDate).getTime());
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Dataset<Row> createMockDataset(PipelineContext context){

        final StructType schema = new StructType(
            new StructField[] {
                DataTypes.createStructField("col1", DataTypes.DateType, false),
                DataTypes.createStructField("col2", DataTypes.LongType, false),
                DataTypes.createStructField("col3", DataTypes.createDecimalType(10,2), false),
                DataTypes.createStructField("col3", DataTypes.StringType, false)
            }
        );

        final List<Row> rows = Arrays.asList(
            RowFactory.create(
                stringToSqlDate("2022-10-04"),
                1L, 
                BigDecimal.valueOf(-259.77), 
                "text 1"), 

            RowFactory.create(
                stringToSqlDate("2022-10-03"),
                2L, 
                BigDecimal.valueOf(2077.77), 
                "text 2")
        );

        return context.sparkSession().createDataFrame(rows,schema);


    }
}