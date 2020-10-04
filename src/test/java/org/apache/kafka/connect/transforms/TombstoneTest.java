package org.apache.kafka.connect.transforms;

import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TombstoneTest {
	
    private final Tombstone<SinkRecord> xform = new Tombstone.Value<>();

    private Map<String, String> props = new HashMap<>(2);

    @Before
    public void setup() {
    	props.put("field", "aField");
    	props.put("value", "aValue");
    	
    	xform.configure(props);
    }
    
    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
    	
    	Map<String, String> testMap = new HashMap<>(2);
    	testMap.put("field", "aField");
    	testMap.put("value", "aValue");
    	
        final SinkRecord record = new SinkRecord("test", 0, null, testMap, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
    }

    @Test
    public void testNullSchemaless() {

        final Map<String, Object> key = null;
        final SinkRecord record = new SinkRecord("test", 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
    }

    @Test
    public void withSchema() {

        final Schema keySchema = SchemaBuilder.struct().field("aField", Schema.STRING_SCHEMA).build();
        final Struct key = new Struct(keySchema).put("aField", "aValue");
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
    }

    @Test
    public void testNullWithSchema() {

    	final Schema keySchema = SchemaBuilder.struct().field("aField", Schema.STRING_SCHEMA).optional().build();
        final Struct key = null;
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
    }
}
