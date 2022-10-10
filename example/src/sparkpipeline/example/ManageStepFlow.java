package sparkpipeline.example;

import sparkpipeline.core.context.PipelineContext;
import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.pipeline.Pipeline;

class ManageStepFlow {


    @SuppressWarnings({"java:S106","java:S1192","java:S1612"})
    public static void main(String[] args) {


        PipelineContextBuilder contextBuilder = PipelineContextBuilder.init()
            .setMaxAmountReRunEachStep(3)
            .setMaxAmountReRunPipeline(2);

        Pipeline.initWithContextBuilder(contextBuilder)
                // rerun current step
                .anyRunning(context -> {
                    printCurrentStepPosition(context);
                    context.controllerExecution().reRunCurrentStep();
                })
                // rerun all pipeline
                .anyRunning(context -> {
                    printCurrentStepPosition(context);
                    context.controllerExecution().reRunAllPipeline();
                })
                // abort all pipeline
                .anyRunning(context -> {
                    printCurrentStepPosition(context);
                    context.controllerExecution().abortPipeline();
                })
                // step wont run
                .anyRunning(context -> {
                    printCurrentStepPosition(context);
                    System.out.println("this step wont be run because pipeline has been aborted in previous one");
                })
                .execute();
    }

    @SuppressWarnings("java:S106")
    private static void printCurrentStepPosition(PipelineContext context){
        System.out.println(
            String.format("step %d of %d from pipeline %d",
                    context.controllerExecution().getCurrentPosition(),
                    context.controllerExecution().getTimesAlreadyRunStep(context.controllerExecution().getCurrentPosition()),
                    context.controllerExecution().getTimesAlreadyRunPipeline())
                    );
    }
}