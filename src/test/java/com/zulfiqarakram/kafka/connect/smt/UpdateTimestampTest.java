package com.zulfiqarakram.kafka.connect.smt;

import com.github.zulfiqarakram.kafka.connect.smt.UpdateTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class UpdateTimestampTest {

    private UpdateTimestamp<SinkRecord> xform = new UpdateTimestamp.Value<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "timestamp1,timestamp2");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("id", "65a861781b01841056e8dac4bd4bcbfd");
        value.put("timestamp1", 131781115208L);
        value.put("timestamp2", 131781115208L);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();

        DateFormat df = new SimpleDateFormat("dd:MM:yy:HH:mm");
        Date dt1=new Date((long)updatedValue.get("timestamp1"));
        Date dt2=new Date((long)updatedValue.get("timestamp2"));
        Date exp_dt=new Date(1583369573386L);
        assertEquals(df.format(exp_dt.getTime()), df.format(dt1.getTime()));
        assertEquals(df.format(exp_dt.getTime()), df.format(dt2.getTime()));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "timestamp1,timestamp2");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("timestamp1", Schema.INT64_SCHEMA)
                .field("timestamp2", Schema.INT64_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("id", "65a861781b01841056e8dac4bd4bcbfd");
        value.put("timestamp1", 131781115208L);
        value.put("timestamp2", 131781115208L);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        DateFormat df = new SimpleDateFormat("dd:MM:yy:HH:mm");
        Date dt1=new Date((long)updatedValue.get("timestamp1"));
        Date dt2=new Date((long)updatedValue.get("timestamp2"));
        Date exp_dt=new Date(1583369573386L);
        assertEquals(df.format(exp_dt.getTime()), df.format(dt1.getTime()));
        assertEquals(df.format(exp_dt.getTime()), df.format(dt2.getTime()));
    }

}