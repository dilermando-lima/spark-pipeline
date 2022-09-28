import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import sparkpipeline.core.context.CommandQueue;
import sparkpipeline.core.context.CommandQueue.TypeCommand;
import sparkpipeline.core.context.DatasetStore;
import sparkpipeline.core.context.DatasetStoreCommand;
import sparkpipeline.core.context.PipelineContext;

class ValidateLogTest {

    @Test
    void validateAnyThing() {

        CommandQueue c = new CommandQueue(new DatasetStore());

        c.addCommand(new DatasetStoreCommand() {

            @Override
            public Consumer<DatasetStore> changeDatasetStore(PipelineContext context) {
                return (datasetStore) -> datasetStore.remove("any");
            }

        }, TypeCommand.ANY_RUNNING);

        assertNotNull(c);

    }

}
