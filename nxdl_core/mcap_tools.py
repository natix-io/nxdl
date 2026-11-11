import logging
from mcap.writer import Writer
from google.protobuf.json_format import ParseDict

logger = logging.getLogger('mcap_tools')

# MCAP stores message times as uint64 nanoseconds.
_MAX_MCAP_TIME_NS = 2**64 - 1


def generate_mcap_file(mcap_file_path, json_data, proto_schema, schema_name, topic_name, log_time_ns: int = 0):
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
        
        # `log_time_ns` is the DATA's time, supplied by the caller, never the wall
        # clock. Stamping `time.time()` here made every delivered MCAP different
        # from the last one generated from the same telemetry, so a re-delivered
        # trip could not be checksum-verified against the original, and the
        # timestamps inside said when the file was written rather than when the
        # data was recorded. 0 is the honest value where the payload carries no
        # time of its own (the trip-level metadata record).
        # MCAP timestamps are uint64 nanoseconds. The telemetry parser accepts
        # dates the format cannot hold (year 2600 overflows), and struct.pack
        # would raise from inside the writer with a message naming neither the
        # file nor the row. Clamp and say so instead of failing the trip.
        stamp = log_time_ns
        if not 0 <= stamp <= _MAX_MCAP_TIME_NS:
            logger.warning(
                "Timestamp %d is outside what MCAP can store; clamping in %s",
                stamp,
                mcap_file_path,
            )
            stamp = min(max(stamp, 0), _MAX_MCAP_TIME_NS)
        writer.add_message(
            channel_id=channel_id,
            data=serialized_data,
            log_time=stamp,
            publish_time=stamp,
            sequence=0
        )

        writer.finish()
    logger.info(f"writing {mcap_file_path} has been finished!")