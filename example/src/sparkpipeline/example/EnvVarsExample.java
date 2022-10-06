package sparkpipeline.example;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import sparkpipeline.core.context.PipelineContext;
import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;
import sparkpipeline.core.vars.VarCollector;
import sparkpipeline.core.writer.WriterCSV;

class EnvVarsExample {

    @SuppressWarnings({"java:S106","java:S1192"})
    public static void main(String[] args) {

        final String DATASET_1 = "DATASET_1_NAME";


        Map<String, Object> varMap = new HashMap<>();
        varMap.put("ENVIRONMENT", "prd");

        PipelineContextBuilder contextBuilder = PipelineContextBuilder
                .init()
                .collectVarsFromArgs(args) // collect vars from application vars
                .collectVarsFromMap(varMap) // collect from map
                .collectVarsFromFile("vars1-prd.env") // collect from file
                .collectVarsFromFile("vars2-${ENVIRONMENT}.properties") // collect from file using var already added before
                .addVarCollector(EnvVarsExample::collectorMethodToUseOnReference) // collect with method reference
                .addVarCollector(pipelineContext -> { // collect using lambda expressions
                    Map<String, Object> varMapLambda = new HashMap<>();
                    varMapLambda.put("TIME_NOW_IN_LAMBDA", LocalDateTime.now());
                    return varMapLambda;
                })
                .addVarCollector(new MyCollectorVars()); // collect using customized implementation

        Pipeline.initWithContextBuilder(contextBuilder)
                .anyRunning(context -> {
                    System.out.println("==== PRINT COLLECTED VARS ======");
                    System.out.println("ENVIRONMENT: "                  + context.varByKey("ENVIRONMENT"));
                    System.out.println("TIME_NOW_IN_LAMBDA: "           +  context.varByKey("TIME_NOW_IN_LAMBDA"));
                    System.out.println("TIME_NOW_IN_METHOD_REFERENCE: " +  context.varByKey("TIME_NOW_IN_METHOD_REFERENCE"));
                    System.out.println("TIME_NOW_IN_MY_COLLECTOR: "     +  context.varByKey("TIME_NOW_IN_MY_COLLECTOR"));
                    System.out.println("DATASET_1_PATH_INPUT: "         +  context.varByKey("DATASET_1_PATH_INPUT"));
                    System.out.println("DATASET_1_TRANSF_PATH_OUTPUT: " +  context.varByKey("DATASET_1_TRANSF_PATH_OUTPUT"));
                    System.out.println("====                      ======");
                })
                .read(DATASET_1, ReaderCSV.init("${DATASET_1_PATH_INPUT}").hasHeader(true))
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())
                .transformSql(DATASET_1, context -> String.format("select * from %s where category <> 'D'",DATASET_1))
                .persist(DATASET_1)
                .anyRunning(context -> context.datasetByKey(DATASET_1).show())
                .write(DATASET_1, WriterCSV.init("${DATASET_1_TRANSF_PATH_OUTPUT}"))
                .execute();

    }


    private static Map<String, Object> collectorMethodToUseOnReference(PipelineContext context) {
        Map<String, Object> varMap = new HashMap<>();
        if ("PRD".equals(context.varByKey("ENVIRONMENT", String.class))) {
            varMap.put("TIME_NOW_IN_METHOD_REFERENCE", LocalDateTime.now());
        }
        return varMap;
    }

    public static class MyCollectorVars implements VarCollector {
        @Override
        public Map<String, Object> collectVarMap(PipelineContext pipelineContext) {
            Map<String, Object> varMapMyOwnCollector = new HashMap<>();
            varMapMyOwnCollector.put("TIME_NOW_IN_MY_COLLECTOR", LocalDateTime.now());
            return varMapMyOwnCollector;
        }
    }
}