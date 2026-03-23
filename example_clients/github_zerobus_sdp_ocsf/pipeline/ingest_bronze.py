"""
SDP Bronze Layer — temporary view over the Zerobus-populated bronze table.

Zerobus (push_zerobus.py) writes directly into github_events_bronze.
This temporary view wraps the external table so it appears in the SDP
pipeline graph without duplicating any data.
"""

from pyspark import pipelines as sdp

from utils_pipeline import TABLES, FQN


@sdp.temporary_view(name=TABLES["bronze"])
def github_events_bronze():
    """Passthrough view over the Zerobus-populated bronze table."""
    return spark.readStream.table(FQN["bronze"])
