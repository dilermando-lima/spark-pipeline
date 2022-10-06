package sparkpipeline.example;

import sparkpipeline.core.pipeline.Pipeline;

class ManageStepFlow {


    @SuppressWarnings({"java:S106","java:S1192","java:S1612"})
    public static void main(String[] args) {
        Pipeline.init()
                // rerun current step
                .anyRunning(context -> {
                    System.out.println(" ==== STEP 2");

                    context.newVar("COUNT_RUN_STEP_2", context.varByKey("COUNT_RUN_STEP_2",Integer.class) + 1);
                   
                    if( context.varByKey("COUNT_RUN_STEP_2",Integer.class) <= 3 ){
                        System.out.println(" ==== STEP WILL BE RUN MORE ONE TIME");
                        context.executionReRunCurrentOne(3);
                    }
                })
                // rerun all pipeline
                .anyRunning(context -> {
                    System.out.println(" ==== STEP 3");

                    context.newVar("COUNT_RUN_PIPELINE", context.varByKey("COUNT_RUN_PIPELINE",Integer.class) + 1);
                   
                    if( context.varByKey("COUNT_RUN_PIPELINE",Integer.class) <= 1 ){
                        context.executionReRunAllPipeline(1);
                    }

                })
                // abort all pipeline
                .anyRunning(context -> context.executionAbortAllPipeline())
                .anyRunning(context -> System.out.println("this step wont be run because pipeline has been aborted in previous one"))
                .execute();
    }
}