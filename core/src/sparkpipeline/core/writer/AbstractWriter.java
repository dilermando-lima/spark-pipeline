package sparkpipeline.core.writer;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sparkpipeline.core.context.PipelineContext;

public abstract class AbstractWriter<W> {

    private PipelineContext context;
    private Predicate<PipelineContext> mockEnableBuilder = c -> false;
    private Consumer<PipelineContext> mockWriterBuilder;
    private BiConsumer<Throwable,PipelineContext> whenThrowError = null;

    public W overrideContext(PipelineContext context) {
        Objects.requireNonNull(context, "context cannot be null");
        this.context = context;
        return returnChildImplementation();
    }

    public W mockEnable(Predicate<PipelineContext> mockEnableBuilder) {
        Objects.requireNonNull(mockEnableBuilder, "mockEnableBuilder cannot be null");
        this.mockEnableBuilder = mockEnableBuilder;
        return returnChildImplementation();
    }

    public W mockWriter(Consumer<PipelineContext> mockWriterBuilder) {
        Objects.requireNonNull(mockWriterBuilder, "mockWriterBuilder cannot be null");
        this.mockWriterBuilder = mockWriterBuilder;
        return returnChildImplementation();
    }

    public W whenThrowError(BiConsumer<Throwable,PipelineContext> whenThrowError){
        this.whenThrowError = whenThrowError;
        return returnChildImplementation();
    }

    abstract BiConsumer<PipelineContext, Dataset<Row>> writeImplementation();

    abstract W returnChildImplementation();

    @SuppressWarnings("java:S1181")
    public void write(Dataset<Row> dataset) {
        Objects.requireNonNull(context, "context cannot be null");
        if (mockEnableBuilder.test(context)) {
            mockWriterBuilder.accept(context);
        } else {
            Objects.requireNonNull(writeImplementation(), "writeImplementation() cannot return null");
            
            try{
                writeImplementation().accept(context, dataset);
            }catch(Throwable throwable){
                if( whenThrowError != null ) {
                    whenThrowError.accept(throwable, context);
                }else{
                    throw throwable;
                }
            }
        }
    }

}
