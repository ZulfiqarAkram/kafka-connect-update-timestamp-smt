Kafka Connect SMT to add a 16801 days in a timestamp.

This SMT supports to update timestamp of a record Value
Property:

|Name|Description|Type|Condition|Importance|
|---|---|---|---|---|
|`fields`| Field name for schema | List | `fields must contain a valid comma separated values as shown in the example below` | High |

Example on how to add to your connector:
```
transforms = updateTimestamp
transforms.updateTimestamp.type = com.github.zulfiqarakram.kafka.connect.smt.UpdateTimestamp$Value
transforms.updateTimestamp.fields = created_on,updated_on
```

Lots borrowed from the Apache Kafka® `MaskedField` SMT