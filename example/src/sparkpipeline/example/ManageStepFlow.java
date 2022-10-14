package sparkpipeline.example;

import org.apache.logging.log4j.Level;

import sparkpipeline.core.context.PipelineContext;
import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.log.LogConfig;
import sparkpipeline.core.pipeline.Pipeline;
import sparkpipeline.core.reader.ReaderCSV;
import sparkpipeline.core.writer.WriterCSV;

class ManageStepFlow {


    @SuppressWarnings({"java:S106","java:S1192","java:S1612"})
    public static void main(String[] args) {

        LogConfig logConfig = LogConfig.init()
                .logConsoleLevel("org.apache.spark", Level.OFF) // silent spark logs
                .logConsoleLevel("sparkpipeline.example", Level.INFO);

        PipelineContextBuilder contextBuilder = PipelineContextBuilder.init()
            .setLogConfig(logConfig)
            .setMaxAmountReRunEachStep(3)
            .setMaxAmountReRunPipeline(2);

        Pipeline.initWithContextBuilder(contextBuilder)
                // rerun current step
                .anyRunning(context -> {
                    printCurrentStepPosition(context);
                    context.controllerExecution().reRunCurrentStep();
                })
                // rerun current step when throw reading
                .read("DATASET_1", ReaderCSV.init("any-path-not-found").whenThrowError((error ,context) -> {
                    System.err.println("error in reading = " + error.getLocalizedMessage());
                    printCurrentStepPosition(context);
                    context.controllerExecution().reRunCurrentStep();
                }))
                // rerun current step when throw wrinting
                .write("DATASET_1", WriterCSV.init("any-path-not-found").whenThrowError((error ,context) -> {
                    System.err.println("error in writing = " + error.getLocalizedMessage());
                    printCurrentStepPosition(context);
                    context.controllerExecution().reRunCurrentStep();
                }))
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