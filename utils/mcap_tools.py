import time, logging
from mcap.writer import Writer
from google.protobuf.json_format import ParseDict

logger = logging.getLogger('mcap_tools')

def generate_mcap_file(mcap_file_path, json_data, proto_schema, schema_name, topic_name):
    ParseDict(json_data, proto_schema, ignore_unknown_fields=True) # Add ignore_unknown_fields if needed

    schema_serialized_pb = proto_schema.DESCRIPTOR.file.serialized_pb
    # serializing the proto data
    serialized_data = proto_schema.SerializeToString()
    # writing to mcap file
    with open(mcap_file_path, "wb") as f:
        writer = Writer(f)
        writer.start()
        
        schema_id = writer.register_schema(
            name=schema_name,
            encoding="protobuf",
            data=schema_serialized_pb,
        )
        channel_id = writer.register_channel(
            topic=topic_name,
            schema_id=schema_id,
            message_encoding="protobuf",
        )
        
        writer.add_message(
            channel_id=channel_id,
            data=serialized_data,
            log_time=int(time.time() * 1e9),
            publish_time=int(time.time() * 1e9),
            sequence=0
        )

        writer.finish()
    logger.info(f"writing {mcap_file_path} has been finished!")