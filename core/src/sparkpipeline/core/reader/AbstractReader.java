package sparkpipeline.core.reader;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sparkpipeline.core.context.PipelineContext;

public abstract class AbstractReader<R> {

    private PipelineContext context;
    private Predicate<PipelineContext> mockEnableBuilder = c -> false;
    private Function<PipelineContext, Dataset<Row>> mockReaderBuilder;
    private BiConsumer<Throwable,PipelineContext> whenThrowError = null;

    abstract R returnChildImplementation();

    public R overrideContext(PipelineContext context) {
        Objects.requireNonNull(context, "context cannot be null");
        this.context = context;
        return returnChildImplementation();
    }

    public R whenThrowError(BiConsumer<Throwable,PipelineContext> whenThrowError){
        this.whenThrowError = whenThrowError;
        return returnChildImplementation();
    }

    public R mockEnable(Predicate<PipelineContext> mockEnableBuilder) {
        Objects.requireNonNull(mockEnableBuilder, "mockEnableBuilder cannot be null");
        this.mockEnableBuilder = mockEnableBuilder;
        return returnChildImplementation();
    }

    public R mockReader(Function<PipelineContext, Dataset<Row>> mockReaderBuilder) {
        Objects.requireNonNull(mockReaderBuilder, "mockReaderBuilder cannot be null");
        this.mockReaderBuilder = mockReaderBuilder;
        return returnChildImplementation();
    }

    abstract Function<PipelineContext, Dataset<Row>> readImplementation();

    @SuppressWarnings("java:S1181")
    public Dataset<Row> buildDataset() {
        Objects.requireNonNull(context, "context cannot be null");
        if (mockEnableBuilder.test(context)) {
            return mockReaderBuilder.apply(context);
        } else {
            
            Objects.requireNonNull(readImplementation(), "readImplementation() cannot return null");
            try{
                return readImplementation().apply(context);
            }catch(Throwable throwable){
                if( whenThrowError != null ) {
                    whenThrowError.accept(throwable, context);
                    return null;
                }else{
                    throw throwable;
                }
            }
            
        }
    }
    
}
