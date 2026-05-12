#!/usr/bin/env python3
import os
import json
import asana.rest
import singer
from singer import utils
from singer import metadata
from singer import Transformer
# Import directly before 'from tap_asana.asana import Asana' shadows the 'asana' name in globals
from asana.rest import ApiException as _AsanaApiException
from tap_asana.asana import Asana
from tap_asana.context import Context
import tap_asana.streams  # Load stream objects into Context

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "redirect_uri",
    "refresh_token",
]


LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    """Load schemas for catalog"""
    schemas = {}

    # This schema represents many of the currency values as JSON schema
    # 'number's, which may result in lost precision.
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        schema_name = filename.replace(".json", "")
        with open(path) as file:  # pylint: disable=unspecified-encoding
            try:
                schemas[schema_name] = json.load(file)
            except ValueError:
                pass

    return schemas


def get_discovery_metadata(stream, schema):
    """Generate metadata"""
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), "table-key-properties", stream.key_properties)
    mdata = metadata.write(
        mdata, (), "forced-replication-method", stream.replication_method
    )

    if stream.replication_key:
        mdata = metadata.write(
            mdata, (), "valid-replication-keys", [stream.replication_key]
        )

    for field_name in schema["properties"].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return metadata.to_list(mdata)


def _probe_workspaces():
    """Fetch the list of workspaces from the Asana API.

    Returns the workspace list, or None when no authenticated client is
    available (e.g. during unit tests). Raises RuntimeError immediately
    when the credentials have no access at all (HTTP 402/403).
    """
    if not getattr(Context.asana, "client", None):
        return None
    try:
        return Context.stream_objects["workspaces"]().fetch_workspaces()
    except _AsanaApiException as e:
        if e.status in [402, 403]:
            raise RuntimeError(
                f"HTTP-error-code: {e.status}, Error: The account credentials supplied do not have "
                "'read' access to any of the streams supported by the tap. "
                "Data collection cannot be initiated due to lack of permissions."
            ) from e
        raise


def _check_stream_access(schema_name, stream, workspaces):
    """Probe stream-specific access when required (e.g. Portfolios / HTTP 402).

    Returns True when the stream is accessible or does not require a check.
    Returns False and logs a warning when the stream is inaccessible (HTTP 402/403).
    Re-raises for any other API error.
    """
    if workspaces is None or not stream.requires_access_check:
        return True
    try:
        stream.check_access(workspaces)
        return True
    except _AsanaApiException as e:
        if e.status in [402, 403]:
            LOGGER.warning(
                "Stream '%s' is not accessible (HTTP %s), excluding from catalog.",
                schema_name, e.status,
            )
            return False
        raise


def _build_catalog_entry(schema_name, schema, stream):
    """Build a single Singer catalog entry dict from schema and stream metadata."""
    return {
        "stream": schema_name,
        "tap_stream_id": schema_name,
        "schema": singer.resolve_schema_references(schema, {}),
        "metadata": get_discovery_metadata(stream, schema),
        "key_properties": stream.key_properties,
        "replication_key": stream.replication_key,
        "replication_method": stream.replication_method,
    }


def _handle_excluded_streams(error_list, streams):
    """Raise RuntimeError when no streams are accessible; otherwise warn.

    Mutates nothing — callers are responsible for the streams list.
    """
    if not error_list:
        return
    excluded_streams = ", ".join(name for name, _ in error_list)
    status_codes = "/".join(str(s) for s in sorted({status for _, status in error_list}))
    if not streams:
        raise RuntimeError(
            f"HTTP-error-code: {status_codes}, Error: The account credentials supplied do not have "
            "'read' access to any of the streams supported by the tap. "
            "Data collection cannot be initiated due to lack of permissions."
        )
    LOGGER.warning(
        "The account credentials supplied do not have 'read' access to the following "
        "stream(s): %s. These streams have been excluded from the catalog.",
        excluded_streams,
    )


def discover():
    """Build and return the Singer catalog.

    Probes workspace and per-stream access when a client is present;
    streams that are inaccessible (HTTP 402/403) are excluded from the
    catalog and a warning is logged for each.
    """
    LOGGER.info("Starting discover")
    raw_schemas = load_schemas()
    workspaces = _probe_workspaces()

    streams = []
    error_list = []

    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        if not _check_stream_access(schema_name, stream, workspaces):
            error_list.append((schema_name, None))
            continue

        streams.append(_build_catalog_entry(schema_name, schema, stream))

    _handle_excluded_streams(error_list, streams)
    LOGGER.info("Finished discover")
    return {"streams": streams}


def shuffle_streams(stream_name):
    """
    Takes the name of the first stream to sync and reshuffles the order
    of the list to put it at the top
    """
    matching_index = 0
    for i, catalog_entry in enumerate(Context.catalog["streams"]):
        if catalog_entry["tap_stream_id"] == stream_name:
            matching_index = i
    top_half = Context.catalog["streams"][matching_index:]
    bottom_half = Context.catalog["streams"][:matching_index]
    Context.catalog["streams"] = top_half + bottom_half


def _emit_schemas():
    """Write Singer schema messages for every selected stream in the catalog."""
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            singer.write_schema(
                stream["tap_stream_id"],
                stream["schema"],
                stream["key_properties"],
            )
            Context.counts[stream["tap_stream_id"]] = 0


def _sync_stream(catalog_entry):
    """Sync a single stream: transform each record, write it, and update state."""
    stream_id = catalog_entry["tap_stream_id"]
    stream = Context.stream_objects[stream_id]()

    LOGGER.info("Syncing stream: %s", stream_id)

    if not Context.state.get("bookmarks"):
        Context.state["bookmarks"] = {}
    Context.state["bookmarks"]["currently_sync_stream"] = stream_id

    with Transformer() as transformer:
        for rec in stream.sync():
            extraction_time = singer.utils.now()
            record_schema = catalog_entry["schema"]
            record_metadata = metadata.to_map(catalog_entry["metadata"])
            rec = transformer.transform(rec, record_schema, record_metadata)
            singer.write_record(stream_id, rec, time_extracted=extraction_time)
            Context.counts[stream_id] += 1

    Context.state["bookmarks"].pop("currently_sync_stream")
    singer.write_state(Context.state)


def _log_sync_counts():
    """Log the number of records synced per stream."""
    LOGGER.info("----------------------")
    for stream_id, stream_count in Context.counts.items():
        LOGGER.info("%s: %d", stream_id, stream_count)
    LOGGER.info("----------------------")


def sync():
    """Sync all selected streams in the catalog.

    Emits schemas first so child streams receive their parent schema,
    then iterates over the catalog and syncs each selected stream.
    """
    _emit_schemas()

    for catalog_entry in Context.catalog["streams"]:
        stream_id = catalog_entry["tap_stream_id"]
        if not Context.is_selected(stream_id):
            LOGGER.info("Skipping stream: %s", stream_id)
            continue
        _sync_stream(catalog_entry)

    _log_sync_counts()


@utils.handle_top_exception(LOGGER)
def main():
    """
    Run discover mode or sync mode.
    """
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Set context.
    creds = {
        "client_id": args.config["client_id"],
        "client_secret": args.config["client_secret"],
        "redirect_uri": args.config["redirect_uri"],
        "refresh_token": args.config["refresh_token"],
    }

    # As we passed 'request_timeout', we need to add a whole 'args.config' rather than adding 'creds'
    Context.config = args.config
    Context.state = args.state
    Context.asana = Asana(**creds)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()
        sync()


if __name__ == "__main__":
    main()
