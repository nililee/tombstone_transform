package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class Tombstone<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Nullify the entire value to make tombstone record if the specified field equals the specified value";

    private static final String FIELD_CONFIG = "field";
    private static final String VALUE_CONFIG = "value";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Field name to extract.")
    		.define(VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "value to compare with.");

    private static final String PURPOSE = "make tombstone record";

    private String fieldName;
    private String valueToCompare;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName      = config.getString(FIELD_CONFIG);
        valueToCompare = config.getString(VALUE_CONFIG);
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
            return newRecord(record, null, value == null ? null : value.get(fieldName));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Field field = schema.field(fieldName);

            if (field == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
            
            return newRecord(
            	record,
            	field.schema(),
            	value.get(fieldName) != null && String.valueOf(value.get(fieldName)).equals(valueToCompare) ? null : value
            );
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends Tombstone<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
