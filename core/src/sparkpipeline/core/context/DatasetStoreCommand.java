package sparkpipeline.core.context;

import java.util.function.Consumer;

public interface DatasetStoreCommand {
    public Consumer<DatasetStore> changeDatasetStore(PipelineContext context);
}
