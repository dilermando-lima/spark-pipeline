package sparkpipeline.core.context;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sparkpipeline.core.constant.Msg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatasetStore {
    static final Logger logger = LoggerFactory.getLogger(DatasetStore.class);
    private final Map<String, Dataset<Row>> storeMap = new HashMap<>();

    public void save(String keyDataset, Dataset<Row> dataset) {
        logger.info("Saving dataset : keyDataset = {}", keyDataset);
        Objects.requireNonNull(keyDataset, Msg.KEY_DATASET_CANNOT_BE_NULL);
        Objects.requireNonNull(dataset, Msg.DATASET_CANNOT_BE_NULL);
        storeMap.put(keyDataset, dataset);
        dataset.createOrReplaceTempView(keyDataset);
    }

    public Dataset<Row> datasetByKey(String keyDataset) {
        Objects.requireNonNull(keyDataset, Msg.KEY_DATASET_CANNOT_BE_NULL);
        return storeMap.get(keyDataset);
    }

    private void validateArrayKeyDataset(String... keyDataset) {
        Objects.requireNonNull(keyDataset, Msg.KEY_DATASET_CANNOT_BE_NULL);
        if (Arrays.stream(keyDataset).anyMatch(Objects::isNull))
            throw new NullPointerException(Msg.KEY_DATASET_CANNOT_HAS_ANY_NULL_ELEMENTS);
    }

    public void persist(String... keyDataset) {
        validateArrayKeyDataset(keyDataset);

        logger.info("Persisting dataset : keyDataset = {}", Arrays.asList(keyDataset));

        for (String key : keyDataset) {
            storeMap.get(key).persist();
        }
    }

    public void unpersist(String... keyDataset) {
        validateArrayKeyDataset(keyDataset);
        logger.info("UnPersisting dataset : keyDataset = {}", Arrays.asList(keyDataset));

        for (String key : keyDataset) {
            storeMap.get(key).unpersist();
        }
    }

    public void remove(String... keyDataset) {
        validateArrayKeyDataset(keyDataset);
        logger.info("Removing dataset : keyDataset = {}", Arrays.asList(keyDataset));

        for (String key : keyDataset) {
            storeMap.get(key).unpersist();
            storeMap.remove(key);
        }
    }

}
