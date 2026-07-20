import os
import logging
import shutil
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
# import exiftool
import subprocess
import math
from schemas.frame_data import FrameData
from schemas.trip_metadata import TripMetadata, CAMERA_TYPE

logger = logging.getLogger("exif_tools")

# def _convert_json_to_xmp(metadata_json):
#     root = ET.Element("x:xmpmeta", {"xmlns:x": "adobe:ns:meta/", "x:xmptk": "Image::ExifTool 12.70"})
#     rdf = ET.SubElement(root, "rdf:RDF", {"xmlns:rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})

#     description = ET.SubElement(rdf, "rdf:Description", {
#         "xmlns:custom": "urn:custom-metadata",
#         "xmlns:exif": "http://ns.adobe.com/exif/1.0/"
#     })

#     for key, value in metadata_json.items():
#         ET.SubElement(description, f"custom:{key}").text = str(value)

#     return ET.tostring(root, encoding="unicode")


# Define namespaces (adjust if needed based on your final JSON keys)
NS = {
    "x": "adobe:ns:meta/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "custom": "urn:custom-metadata",
    "exif": "http://ns.adobe.com/exif/1.0/",
    "aux": "http://ns.adobe.com/exif/1.0/aux/",
    "exifEX": "http://cipa.jp/exif/1.0/",
    "xmp": "http://ns.adobe.com/xap/1.0/",
    "xmpDM": "http://ns.adobe.com/xmp/1.0/DynamicMedia/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

# Register namespaces for cleaner output XML (optional but recommended)
for prefix, uri in NS.items():
    try:
        ET.register_namespace(prefix, uri)
    except ValueError:  # Handle potential re-registration issues if run multiple times
        pass


def _add_dict_to_xml(parent_element, data_dict):
    """Recursively adds dictionary items to an XML element."""
    for key, value in data_dict.items():
        if value is None:  # Skip None values entirely
            continue

        # Handle special structure keys like rdf:Seq
        if key == "rdf:Seq":
            if isinstance(value, list):
                seq_elem = ET.SubElement(parent_element, "rdf:Seq")
                for item in value:
                    li_elem = ET.SubElement(seq_elem, "rdf:li")
                    if isinstance(item, dict):
                        # Recursively add the dictionary content of the list item
                        # We might need rdf:Description inside rdf:li depending on exact XMP spec
                        # For simplicity here, adding directly to li_elem
                        _add_dict_to_xml(li_elem, item)
                    else:
                        # Handle non-dictionary list items if necessary
                        li_elem.text = str(item)
            else:
                logger.warning(
                    f"Expected a list for rdf:Seq, but got {type(value)}")
            continue  # rdf:Seq handled, move to next key

        # Create the element, handling namespace prefixes in the key
        # Assume keys in JSON match desired XML tags (e.g., "exif:GPSLatitude")
        elem = ET.SubElement(parent_element, key)

        if isinstance(value, dict):
            # Recursively add nested dictionary
            _add_dict_to_xml(elem, value)
        elif isinstance(value, list):
            # Handle plain lists - needs clarification on desired XMP structure
            # Often rdf:Bag or rdf:Seq should be used explicitly in the JSON structure
            logger.warning(
                f"Plain list found for key '{key}'. Consider using 'rdf:Seq' structure in JSON.")
            # Example: create rdf:Bag for unordered list
            bag_elem = ET.SubElement(elem, "rdf:Bag")
            for item in value:
                ET.SubElement(bag_elem, "rdf:li").text = str(item)
        else:
            # Assign simple value as text content
            elem.text = str(value)


def _convert_json_to_xmp_nested(metadata_json):
    """Converts a potentially nested Python dict (matching XMP structure) to an XMP string."""
    root = ET.Element(
        "x:xmpmeta", {"xmlns:x": NS["x"], "x:xmptk": "Image::ExifTool 12.70"})
    rdf = ET.SubElement(root, "rdf:RDF", {"xmlns:rdf": NS["rdf"]})

    # Create the main Description element with all necessary namespaces defined
    # Standard attribute for the main description
    description_attrs = {"rdf:about": ""}
    for prefix, uri in NS.items():
        if prefix not in ['x', 'rdf']:  # Avoid redefining x and rdf
            description_attrs[f"xmlns:{prefix}"] = uri

    description = ET.SubElement(rdf, "rdf:Description", description_attrs)

    # Populate the description element from the top-level JSON dictionary
    _add_dict_to_xml(description, metadata_json)

    xml_string = ET.tostring(root, encoding="unicode", method="xml")

    xmp_packet = f"<?xpacket begin='' id='W5M0MpCehiHzreSzNTczkc9d'?>\n{xml_string}\n<?xpacket end='w'?>"
    return xmp_packet

def get_datetime(epoch_time_ms: int, timezone: str, format: str = '%Y:%m:%d %H:%M:%S') -> str:
    return datetime.fromtimestamp(epoch_time_ms / 1000, tz=ZoneInfo(timezone)).strftime(format)

def _convert_all_data_to_json(frame_data_list: list[FrameData], epoch_time_ms: int, timezone: str, camera_type: CAMERA_TYPE, original_filename: str, trip: TripMetadata):
    camera_calibration = trip.get_camera_calibration(camera_type)
    device_calibration = trip.get_device_calibration('vx360')
    phone_calibration = trip.get_device_calibration('phone')

    first_frame = next(
        (frame for frame in frame_data_list
         if frame is not None and frame.GPS_latitude_deg is not None and frame.GPS_longitude_deg is not None), None
    )

    return {
        "custom:metadata_version": trip.version,
        # For all Static EXIF Metadata date, GPS, etc: use start of the clip
        "exif:DateTimeOriginal": get_datetime(epoch_time_ms, timezone, '%Y:%m:%d %H:%M:%S'),
        "exif:SubSecTime": get_datetime(epoch_time_ms, timezone, '%f'),  # TODO:
        "exif:SubSecTimeOriginal": get_datetime(epoch_time_ms, timezone, '%f'),
        "exif:SubSecDateTimeOriginal": get_datetime(epoch_time_ms, timezone, '%Y:%m:%d %H:%M:%S.%f'),
        "exif:SubSecCreateDate": get_datetime(epoch_time_ms, timezone, '%Y:%m:%d %H:%M:%S.%f'),
        "exif:SubSecModifyDate": get_datetime(epoch_time_ms, timezone, '%Y:%m:%d %H:%M:%S.%f'),
        # "custom:time_err_ms": all_sensor_data[0]["time_err_offset_ms"],

        # GPS Data
        "exif:GPSLatitudeRef": "N",
        "exif:GPSLongitudeRef": "E",
        "exif:GPSLatitude": first_frame.GPS_latitude_deg if first_frame else None,
        "exif:GPSLongitude": first_frame.GPS_longitude_deg if first_frame else None,
        "exif:GPSPosition": f"{first_frame.GPS_latitude_deg if first_frame else None}, {first_frame.GPS_longitude_deg if first_frame else None}",

        # Lens and Image Info
        # or Rear Camera, Left Camera, Right Camera
        "exifEX:CameraLabel": f"{camera_type.value.upper()} Camera",
        "exif:FocalLength": math.sqrt(camera_calibration["fx"] * camera_calibration["fy"]),
        "aux:Lens": f"{camera_calibration['k1']} {camera_calibration['k2']}",
        "exif:ImageWidth": trip.frame_width,
        "exif:ImageHeight": trip.frame_height,
        "custom:Megapixels": trip.frame_mp,

        # TODO: check if we have to use original_filename or final_filename
        "dc:title": original_filename,
        "custom:platform_type_gps": "VX360",
        "custom:trip_identifier": trip.trip_id,
        "custom:vehicle_make": trip.vehicle_make,
        "custom:vehicle_model": trip.vehicle_model,
        "custom:phone_make": trip.phone_make,
        "custom:phone_model": trip.phone_model,
        "custom:platform_type_video": trip.platform_type_video,
        "xmpDM:Platform": trip.platform_type_data,
        "custom:reference_frame": trip.reference_frame,

        # Camera Calibration Data - use the camera's values from which this video was shot from. e.g. if Front Camera
        "custom:CameraCalibration": {
            "rdf:Seq": [
                {
                    "custom:device_name": camera_calibration["device_name"],
                    "custom:extrinsics": {
                        "r11": camera_calibration["r11"], "r12": camera_calibration["r12"], "r13": camera_calibration["r13"], "tx": camera_calibration["tx"],
                        "r21": camera_calibration["r21"], "r22": camera_calibration["r22"], "r23": camera_calibration["r23"], "ty": camera_calibration["ty"],
                        "r31": camera_calibration["r31"], "r32": camera_calibration["r32"], "r33": camera_calibration["r33"], "tz": camera_calibration["tz"],
                        "field_of_view_hor_deg": camera_calibration["field_of_view_hor_deg"], "field_of_view_ver_deg": camera_calibration["field_of_view_ver_deg"]
                    },
                    "custom:intrinsics": {
                        "fx": camera_calibration["fx"], "cx": camera_calibration["cx"], "fy": camera_calibration["fy"], "cy": camera_calibration["cy"],
                        "k1": camera_calibration["k1"], "k2": camera_calibration["k2"], "k3": camera_calibration["k3"],
                        "p1": camera_calibration["p1"], "p2": camera_calibration["p2"]
                    }
                },
                {
                    "custom:device_name": "phone",
                    "custom:extrinsics": {
                        "tx": phone_calibration["tx"],
                        "ty": phone_calibration["ty"],
                        "tz": phone_calibration["tz"]
                    }
                },
                {
                    "custom:device_name": "vx360",
                    "custom:extrinsics": {
                        "tx": device_calibration["tx"],
                        "ty": device_calibration["ty"],
                        "tz": device_calibration["tz"]
                    }
                }
            ]
        },
        # Timed Metadata (Frame-Level Data)
        "custom:FrameMetadata": {
            "rdf:Seq": [
                {
                    "exif:DateTimeOriginal": frame.get_timestamp(format='%Y:%m:%d %H:%M:%S', utc=True),
                    "exif:SubSecDateTimeOriginal": frame.get_timestamp(format='%Y:%m:%d %H:%M:%S.%f', utc=True)[:-3],
                    "exif:SubSecTime": int(frame.get_timestamp(format='%f', utc=True)[:-3]),
                } for frame in frame_data_list
                if any([frame.GPS_latitude_deg, frame.GPS_longitude_deg])
            ]
        }
    }


def embed_xmp_into_video(xmp_file_path: str, video_file_path: str):
    """This methods embed given XMP file into given Video file and deletes the XMP file afterward."""
    try:
        # ? deprecating exiftool python lib to use direct system command to prevent this error:
        # ?  'ValueError: filedescriptor out of range in select()' --> More info: https://discord.com/channels/755797452602474531/1428036398924959777/1439975315274338314
        # with exiftool.ExifTool() as et:
        #     et.execute(b"-overwrite_original", f"-xmp<={xmp_file_path}".encode("utf-8"), video_file_path.encode("utf-8"))
        subprocess.run(["exiftool", f"-XMP<={xmp_file_path}", "-overwrite_original", str(video_file_path)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # remove XMP file
        if os.path.exists(xmp_file_path):
            os.remove(xmp_file_path)
        logger.info(f"Metadata successfully embedded into {video_file_path}")
    except Exception as e:
        # Log the specific error during embedding
        logger.error(
            f"Error embedding metadata into {video_file_path}: {e}", exc_info=True)
        # Re-raise the exception so the caller knows embedding failed
        raise RuntimeError(
            f"Failed to embed metadata into {video_file_path}") from e


def extract_xmp_metadata(frame_data_list: list[FrameData], epoch_time_ms: int, timezone: str, camera_type: CAMERA_TYPE, original_filename: str, trip: TripMetadata, xmp_file_path: str):
    """Extract the required XMP data and store it in given file path"""
    try:
        # convert all footage, trip and telemetry data into EXIF-formatted JSON
        metadata_json = _convert_all_data_to_json(frame_data_list, epoch_time_ms, timezone, camera_type, original_filename, trip)
        # convert the exit-formatted json into xmp format
        xmp_data = _convert_json_to_xmp_nested(metadata_json)
        # Use delete=False because ffmpeg needs to open the file by path
        # with tempfile.NamedTemporaryFile(mode='w', suffix='.xmp', delete=False, encoding='utf-8') as temp_xmp_file:
        with open(file=xmp_file_path, mode="w", encoding='utf-8') as xml_file:
            xml_file.write(xmp_data)
    except Exception as e:
        logger.error(f"extract_xmp_metadata {xml_file}: {e}")
