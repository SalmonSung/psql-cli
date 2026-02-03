from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Union


@dataclass
class TimeSeries:
    """
    Generic time series container.

    values: list of (timestamp, value) tuples
    unit: optional unit string (e.g. 'ratio', 'bytes')
    """
    values: List[Tuple[datetime, Union[float, int, bool]]] = field(default_factory=list)
    unit: Optional[str] = None

    def add(self, ts: datetime, value: Union[float, int, bool]):
        self.values.append((ts, value))

    def timestamps(self):
        return [t for t, _ in self.values]

    def data(self, copy: bool = False):
        if copy:
            return [v for _, v in self.values].copy()
        else:
            return [v for _, v in self.values]

    def copy(self) -> TimeSeries:
        return TimeSeries(
            values=[(ts, v) for ts, v in self.values],
            unit=self.unit,
        )

    def sort(self, ascending: bool = True):
        """
        Sort the time series values by timestamp.

        :param ascending: If True, sort oldest → newest. If False, newest → oldest.
        """
        self.values.sort(key=lambda x: x[0], reverse=not ascending)

    def get_by_ts(self, ts: datetime) -> Union[float, int, bool]:
        """
        Get value for an exact timestamp.
        Returns 0 if the timestamp does not exist.
        """
        for t, v in self.values:
            if t == ts:
                return v
        return 0

    def group_by_minutes(self, min: int, mode: str = "sum") -> None:
        """
        Group values into fixed minute buckets aligned to multiples of `min`
        (e.g. 12:00, 12:05, 12:10 for min=5), and REPLACE self.values in-place.

        :param min: Bucket size in minutes
        :param mode: Aggregation mode: "sum" or "avg"
        """
        if min <= 0:
            raise ValueError("min must be > 0")
        if mode not in ("sum", "avg"):
            raise ValueError("mode must be 'sum' or 'avg'")

        sums: Dict[datetime, float] = {}
        counts: Dict[datetime, int] = {}

        for ts, value in self.values:
            bucket_minute = (ts.minute // min) * min
            bucket_ts = ts.replace(minute=bucket_minute, second=0, microsecond=0)

            sums[bucket_ts] = sums.get(bucket_ts, 0.0) + float(value)
            counts[bucket_ts] = counts.get(bucket_ts, 0) + 1

        new_values: List[Tuple[datetime, Union[float, int, bool]]] = []
        for ts in sorted(sums.keys()):
            if mode == "sum":
                agg = sums[ts]
            else:  # avg
                agg = sums[ts] / counts[ts]
            new_values.append((ts, agg))

        self.values = new_values

    def combine(
            self,
            other: "TimeSeries",
            mode: str = "sum"
    ) -> "TimeSeries":
        if mode not in ("sum", "avg"):
            raise ValueError("mode must be 'sum' or 'avg'")

        result = TimeSeries(unit=self.unit or other.unit)

        # Build timestamp → value maps
        self_map: Dict[datetime, Union[float, int, bool]] = {
            ts: v for ts, v in self.values
        }
        other_map: Dict[datetime, Union[float, int, bool]] = {
            ts: v for ts, v in other.values
        }

        all_timestamps = set(self_map) | set(other_map)

        for ts in all_timestamps:
            v1 = self_map.get(ts)
            v2 = other_map.get(ts)

            if v1 is not None and v2 is not None:
                if mode == "sum":
                    value = v1 + v2
                else:  # avg
                    value = (v1 + v2) / 2
            else:
                value = v1 if v1 is not None else v2

            result.add(ts, value)

        result.sort()
        return result

    def extend(self, timeseries: TimeSeries):
        self.values.extend(timeseries.values)

@dataclass
class PerqueryLockTimeMetric:
    querystring: Optional[str] = None
    query_hash: Optional[str] = None
    user: Optional[str] = None
    location: Optional[str] = None
    database: Optional[str] = None
    total_wait: int | None = None

    perquery_lock_time: TimeSeries = field(default_factory=lambda: TimeSeries(unit="us = microseconds (1,000,000 µs = 1 second)"))
    pass

@dataclass
class PerqueryLatencyMetric:
    querystring: Optional[str] = None
    query_hash: Optional[str] = None
    user: Optional[str] = None
    location: Optional[str] = None
    database: Optional[str] = None

    perquery_count: TimeSeries = field(default_factory=lambda: TimeSeries(unit="count"))
    perquery_latency_mean: TimeSeries = field(default_factory=lambda: TimeSeries(unit="us = microseconds"))
    perquery_latency_pr75: TimeSeries = field(default_factory=lambda: TimeSeries(unit="us = microseconds"))

@dataclass
class PerqueryIOTimeMetric:
    user: Optional[str] = None
    querystring: Optional[str] = None
    io_type: Optional[str] = None
    query_hash: Optional[str] = None
    database: Optional[str] = None
    perquery_IO_time: TimeSeries = field(default_factory=lambda: TimeSeries(unit="us = microseconds"))


@dataclass
class WALFlushedBytesCountMetric:
    database_id: Optional[str] = None
    region: Optional[str] = None

    wal_flushed_bytes_count: TimeSeries = field(default_factory=lambda: TimeSeries(unit="bytes/min"))

@dataclass
class WALInsertedBytesCountMetric:
    database_id: Optional[str] = None
    region: Optional[str] = None

    wal_inserted_bytes_count: TimeSeries = field(default_factory=lambda: TimeSeries(unit="bytes/min"))

@dataclass
class PSQLNumBackendsByStateMetric:
    state: Optional[str] = None
    database: Optional[str] = None
    region: Optional[str] = None

    psql_num_backends_by_state: TimeSeries = field(default_factory=lambda: TimeSeries(unit="counts"))

@dataclass
class CloudSQLMetrics:
    """
    Collected metrics for a single Cloud SQL instance and time window.
    """
    perquery_lock_time_metrics: List[PerqueryLockTimeMetric] = field(default_factory=list)
    perquery_latency_metrics: List[PerqueryLatencyMetric] = field(default_factory=list)
    perquery_IO_time_metrics: List[PerqueryIOTimeMetric] = field(default=list)
    wal_flushed_bytes_metrics: WALFlushedBytesCountMetric = field(
        default_factory=WALFlushedBytesCountMetric
    )
    wal_inserted_bytes_metrics: WALInsertedBytesCountMetric = field(
        default_factory=WALInsertedBytesCountMetric
    )
    psql_num_backends_by_state_metrics: List[PSQLNumBackendsByStateMetric] = field(
        default_factory=PSQLNumBackendsByStateMetric
    )

    cpu_usage_time: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="CPU-seconds")
    )
    cpu_utilization: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="ratio")
    )
    cpu_reserved_cores: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="core")
    )

    disk_quota: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="bytes")
    )
    disk_utilization: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="ratio")
    )
    disk_read_bytes: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="bytes")
    )
    disk_read_ops: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="time")
    )
    disk_write_bytes: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="bytes")
    )
    disk_write_ops: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="time")
    )
    disk_bytes_used: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="bytes")
    )
    disk_bytes_used_by_type: Dict[str, TimeSeries] = field(default_factory=dict)

    memory_quota: TimeSeries = field(
        default_factory=lambda: TimeSeries(unit="bytes")
    )
    memory_components: Dict[str, TimeSeries] = field(default_factory=dict)


    pg_stat_statements_top_queries: List[Dict] = field(default_factory=list)
    pg_stat_statements_heavy_wal: List[Dict] = field(default_factory=list)


if __name__ == "__main__":
    pass
    # metrics = CloudSQLMetrics()
    #
    # for ts in series:  # list_time_series result
    #     for p in ts.points:
    #         metrics.cpu_utilization.add(
    #             p.interval.end_time,
    #             p.value.double_value
    #         )