import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Any, Callable, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import monitoring_v3

from metrics import *


class GMonitoringCollector:
    def __init__(self, project_id: str, instance_id: str, duration_hours: int, start_time: Optional[datetime] = None,
                 end_time: Optional[datetime] = None, max_workers: int = 8):
        self.project_id = project_id
        self.instance_id = instance_id
        self.duration_hours = duration_hours
        self.start_time = start_time
        self.end_time = end_time
        self.max_workers = max_workers
        self._monitoring_client = monitoring_v3.MetricServiceClient()

    def get_start_end_time(self) -> Tuple[datetime, datetime]:
        # Case 1: explicit start and end
        if self.start_time is not None and self.end_time is not None:
            return self.start_time, self.end_time

        # Case 2: start_time + duration
        elif self.start_time is not None:
            start = self.start_time
            end = start + timedelta(hours=self.duration_hours)
            return start, end

        # Case 3: end_time + duration
        else:
            end = self.end_time or datetime.now(timezone.utc)
            start = end - timedelta(hours=self.duration_hours)
            return start, end

    def load_wal_flushed_bytes_count(self) -> WALFlushedBytesCountMetric:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/write_ahead_log/flushed_bytes_count"

        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": {
                "alignment_period": {"seconds": 60},
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
            },
        }

        logging.debug(
            "Loading WAL - Flushed Bytes Count time series (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for WAL - Flushed Bytes Count", len(series_list))

        if not series_list:
            return WALFlushedBytesCountMetric(
                database_id=f"{self.project_id}:{self.instance_id}",
                region=None,
                wal_flushed_bytes_count=TimeSeries(unit="bytes"),
            )

        # Check resource identity (should be identical for all returned series)
        r0 = dict(series_list[0].resource.labels)
        database_id = r0.get("database_id")
        region = r0.get("region")

        metric_obj = WALFlushedBytesCountMetric(
            database_id=database_id,
            region=region,
            wal_flushed_bytes_count=TimeSeries(unit="bytes"),
        )

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                value = p.value.double_value  # RATE returns double

                metric_obj.wal_flushed_bytes_count.add(dt, value)

        return metric_obj

    def load_wal_inserted_bytes_count(self) -> WALInsertedBytesCountMetric:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/write_ahead_log/inserted_bytes_count"

        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": {
                "alignment_period": {"seconds": 60},
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
            },
        }

        logging.debug(
            "Loading WAL - Inserted Bytes Count time series (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for WAL - Inserted Bytes Count", len(series_list))

        if not series_list:
            return WALInsertedBytesCountMetric(
                database_id=f"{self.project_id}:{self.instance_id}",
                region=None,
                wal_inserted_bytes_count=TimeSeries(unit="bytes"),
            )

        # Check resource identity (should be identical for all returned series)
        r0 = dict(series_list[0].resource.labels)
        database_id = r0.get("database_id")
        region = r0.get("region")

        metric_obj = WALInsertedBytesCountMetric(
            database_id=database_id,
            region=region,
            wal_inserted_bytes_count=TimeSeries(unit="bytes"),
        )

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                value = p.value.double_value  # RATE returns double

                metric_obj.wal_inserted_bytes_count.add(dt, value)

        return metric_obj

    def load_perquery_latency(self) -> List[PerqueryLatencyMetric]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/insights/perquery/latencies"
        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_instance_database" '
                f'AND resource.labels.resource_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading perquery latencies series (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for perquery latencies", len(series_list))

        # Group by identifying labels (so each unique query/user/db becomes one object)
        grouped: Dict[Tuple[str, str, str, str, str], PerqueryLatencyMetric] = {}

        def _percentile_from_explicit_buckets(
                bounds: List[float],
                bucket_counts: List[int],
                q: float,
        ) -> float:
            """
            Approximate percentile q in [0,1] from an explicit-bucket histogram.

            Buckets:
              0: (-inf, b0)
              i: [b_{i-1}, b_i) for i=1..n-1
              n: [b_{n-1}, +inf)
            where n = len(bounds)

            We linearly interpolate inside the bucket assuming uniform density.
            For the +inf bucket we return its lower bound.
            """
            total = sum(bucket_counts)
            if total <= 0:
                return 0.0

            # Clamp q
            if q <= 0:
                q = 0.0
            elif q >= 1:
                q = 1.0

            target = q * total
            cum = 0.0

            for i, c in enumerate(bucket_counts):
                if c <= 0:
                    cum += c
                    continue

                next_cum = cum + c
                if target <= next_cum:
                    # target lies in this bucket
                    within = 0.0 if c == 0 else (target - cum) / c  # 0..1

                    # Determine bucket lower/upper
                    if i == 0:
                        # (-inf, b0) -> best we can do is 0..b0, return within*b0
                        upper = float(bounds[0]) if bounds else 0.0
                        return max(0.0, within * upper)

                    n = len(bounds)
                    if i >= n:
                        # last bucket: [b_{n-1}, +inf) -> return lower bound
                        return float(bounds[-1]) if bounds else 0.0

                    lower = float(bounds[i - 1])
                    upper = float(bounds[i])
                    return lower + within * (upper - lower)

                cum = next_cum

            # If we somehow didn't hit (rounding), return last finite bound or 0
            return float(bounds[-1]) if bounds else 0.0

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            rlabels = dict(ts.resource.labels)

            querystring = mlabels.get("querystring")
            query_hash = mlabels.get("query_hash")
            user = mlabels.get("user")
            location = rlabels.get("location")
            database = rlabels.get("database")

            key = (
                query_hash or "",
                querystring or "",
                user or "",
                location or "",
                database or "",
            )

            metric_obj = grouped.get(key)
            if metric_obj is None:
                metric_obj = PerqueryLatencyMetric(
                    querystring=querystring,
                    query_hash=query_hash,
                    user=user,
                    location=location,
                    database=database,
                    # keep units consistent with dataclass defaults
                    perquery_count=TimeSeries(unit="count"),
                    perquery_latency_mean=TimeSeries(unit="us = microseconds"),
                    perquery_latency_pr75=TimeSeries(unit="us = microseconds"),
                )
                grouped[key] = metric_obj

            # Points are DISTRIBUTION and (typically) CUMULATIVE => compute deltas between points.
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            last_count: Optional[int] = None
            last_sum_us: Optional[float] = None  # sum of samples in microseconds (mean * count)
            last_bucket_counts: Optional[List[int]] = None
            last_bounds: Optional[List[float]] = None

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)

                dist = p.value.distribution_value

                cur_count = int(dist.count)
                cur_mean_us = float(dist.mean)  # microseconds
                cur_sum_us = cur_mean_us * cur_count

                # Explicit bucket configuration (needed for pr75)
                bounds: List[float] = []
                buckets: List[int] = []
                try:
                    eb = dist.bucket_options.explicit_buckets
                    bounds = list(eb.bounds)
                    buckets = [int(x) for x in dist.bucket_counts]
                except Exception:
                    # If buckets are unavailable, we can still compute count & mean.
                    bounds = []
                    buckets = []

                # First point: cannot delta against previous; treat as "delta == current" (same as your lock_time logic)
                if last_count is None:
                    delta_count = cur_count
                    delta_sum_us = cur_sum_us
                    delta_buckets = buckets[:] if buckets else []
                    delta_bounds = bounds[:] if bounds else []
                else:
                    delta_count = cur_count - last_count
                    if delta_count < 0:
                        delta_count = 0

                    delta_sum_us = cur_sum_us - (last_sum_us or 0.0)
                    if delta_sum_us < 0:
                        delta_sum_us = 0.0

                    # Bucket-wise delta (only if layout matches)
                    if (
                            buckets
                            and last_bucket_counts
                            and bounds
                            and last_bounds
                            and len(bounds) == len(last_bounds)
                            and len(buckets) == len(last_bucket_counts)
                    ):
                        delta_buckets = []
                        for cur_b, last_b in zip(buckets, last_bucket_counts):
                            d = cur_b - last_b
                            delta_buckets.append(d if d > 0 else 0)
                        delta_bounds = bounds
                    else:
                        delta_buckets = []
                        delta_bounds = bounds[:] if bounds else []

                # perquery_count
                metric_obj.perquery_count.add(dt, int(delta_count))

                # perquery_latency_mean (microseconds)
                if delta_count > 0:
                    mean_us = float(delta_sum_us) / float(delta_count)
                else:
                    mean_us = 0.0
                metric_obj.perquery_latency_mean.add(dt, mean_us)

                # perquery_latency_pr75 (microseconds) from delta histogram
                if delta_count > 0 and delta_buckets and delta_bounds:
                    pr75_us = _percentile_from_explicit_buckets(delta_bounds, delta_buckets, 0.75)
                else:
                    # Fallback if we can't compute from buckets:
                    # with delta_count==1, mean is the best estimate; otherwise 0
                    pr75_us = mean_us if delta_count == 1 else 0.0
                metric_obj.perquery_latency_pr75.add(dt, pr75_us)

                # advance "last" for delta computation
                last_count = cur_count
                last_sum_us = cur_sum_us
                last_bucket_counts = buckets[:] if buckets else []
                last_bounds = bounds[:] if bounds else []

        # Sort each metric chronologically
        result = list(grouped.values())
        for obj in result:
            obj.perquery_count.values.sort(key=lambda tv: tv[0])
            obj.perquery_latency_mean.values.sort(key=lambda tv: tv[0])
            obj.perquery_latency_pr75.values.sort(key=lambda tv: tv[0])

        logging.info(
            "Returning %d perquery latency metrics (unique query/user/location/db buckets)",
            len(result),
        )

        return result

    def load_perquery_lock_time(self) -> list[PerqueryLockTimeMetric]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/insights/perquery/lock_time"

        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_instance_database" '
                f'AND resource.labels.resource_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading perquery lock time series (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for perquery lock time", len(series_list))

        # Group by identifying labels (so each unique query/user/db becomes one object)
        grouped: Dict[Tuple[str, str, str, str, str], PerqueryLockTimeMetric] = {}

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            rlabels = dict(ts.resource.labels)

            querystring = mlabels.get("querystring")
            query_hash = mlabels.get("query_hash")
            user = mlabels.get("user")
            location = rlabels.get("location")
            database = rlabels.get("database")

            key = (
                query_hash or "",
                querystring or "",
                user or "",
                location or "",
                database or "",
            )

            metric_obj = grouped.get(key)
            if metric_obj is None:
                metric_obj = PerqueryLockTimeMetric(
                    querystring=querystring,
                    query_hash=query_hash,
                    user=user,
                    location=location,
                    database=database,
                    perquery_lock_time=TimeSeries(
                        unit="us = microseconds (1,000,000 µs = 1 second)"
                    ),
                )
                grouped[key] = metric_obj

            last_p = None
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                cur = int(p.value.int64_value)
                if last_p is None:
                    delta = cur
                else:
                    delta = cur - last_p
                    if delta < 0:
                        delta = 0
                metric_obj.perquery_lock_time.add(dt, delta)
                last_p = cur

        # Sort each metric chronologically (Cloud Monitoring often returns newest-first)
        result = list(grouped.values())
        for obj in result:
            obj.perquery_lock_time.values.sort(key=lambda tv: tv[0])

        logging.info(
            "Returning %d perquery lock time metrics (unique query/user/location/db buckets)",
            len(result),
        )

        return result

    def load_perquery_IO_time(self) -> list[PerqueryIOTimeMetric]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/insights/perquery/io_time"

        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_instance_database" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading perquery IO time series (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for perquery IO time", len(series_list))

        # Group by identifying labels (so each unique query_hash/io_type/db/user becomes one object)
        grouped: Dict[Tuple[str, str, str, str], PerqueryIOTimeMetric] = {}

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            rlabels = dict(ts.resource.labels)

            querystring = mlabels.get("querystring")
            query_hash = mlabels.get("query_hash")
            io_type = mlabels.get("io_type")
            user = mlabels.get("user")
            database = rlabels.get("database")

            key = (
                query_hash or "",
                io_type or "",
                user or "",
                database or "",
            )

            metric_obj = grouped.get(key)
            if metric_obj is None:
                metric_obj = PerqueryIOTimeMetric(
                    querystring=querystring,
                    query_hash=query_hash,
                    io_type=io_type,
                    user=user,
                    database=database,
                    perquery_IO_time=TimeSeries(
                        unit="us = microseconds (1,000,000 µs = 1 second)"
                    ),
                )
                grouped[key] = metric_obj

            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                metric_obj.perquery_IO_time.add(dt, p.value.int64_value)


        # Sort each metric chronologically (Cloud Monitoring often returns newest-first)
        result = list(grouped.values())
        for obj in result:
            obj.perquery_IO_time.values.sort(key=lambda tv: tv[0])

        logging.info(
            "Returning %d perquery IO time metrics (unique query_hash/io_type/db/user buckets)",
            len(result),
        )

        return result

    def load_psql_num_backends_by_state(self) -> list[PSQLNumBackendsByStateMetric]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/postgresql/num_backends_by_state"

        start_time, end_time = self.get_start_end_time()

        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Network - PostgreSQL num of backends by state (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Network - PostgreSQL num of backends by state", len(series_list))

        if not series_list:
            return [PSQLNumBackendsByStateMetric(
                state="No Data",
                database="No Data",
                region="No Data",
                psql_num_backends_by_state=TimeSeries(unit="counts"),
            )]

        # Group by identifying labels (so each unique query/user/db becomes one object)
        grouped: Dict[Tuple[str, str, str], PSQLNumBackendsByStateMetric] = {}

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            rlabels = dict(ts.resource.labels)

            state = mlabels.get("state")
            database = mlabels.get("database")

            region = rlabels.get("region")

            key = (
                state or "",
                database or "",
                region or "",
            )

            metric_obj = grouped.get(key)
            if metric_obj is None:
                metric_obj = PSQLNumBackendsByStateMetric(
                    state=state,
                    database=database,
                    region=region,
                    psql_num_backends_by_state=TimeSeries(
                        unit="counts",
                    )
                )
                grouped[key] = metric_obj

            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                metric_obj.psql_num_backends_by_state.add(dt, p.value.int64_value)

        result = list(grouped.values())

        logging.info(
            "Returning %d PostgreSQL num of backends by state metrics (state/database/region)",
            len(result),
        )

        return result

    def load_cpu_usage_time(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/cpu/usage_time"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }


        logging.debug(
            "Loading CPU - Usage time (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for CPU - Usage time", len(series_list))

        if not series_list:
            return TimeSeries(unit="CPU-seconds")

        results: TimeSeries = TimeSeries(unit="CPU-seconds")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.double_value)

        logging.info(
            "Returning %d CPU - Usage time",
            len(results.values),
        )

        return results

    def load_cpu_utilization(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/cpu/utilization"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading CPU - Utilization (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for CPU - Utilization", len(series_list))

        if not series_list:
            return TimeSeries(unit="%")

        results: TimeSeries = TimeSeries(unit="%")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.double_value)

        logging.info(
            "Returning %d CPU - Utilization",
            len(results.values),
        )

        return results

    def load_disk_quota(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/disk/quota"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Disk - Quota (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Disk - Quota", len(series_list))

        if not series_list:
            return TimeSeries(unit="bytes")

        results: TimeSeries = TimeSeries(unit="bytes")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.int64_value)

        logging.info(
            "Returning %d Disk - Quota",
            len(results.values),
        )

        return results

    def load_disk_utilization(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/disk/utilization"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Disk - Quota (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Disk - utilization", len(series_list))

        if not series_list:
            return TimeSeries(unit="%")

        results: TimeSeries = TimeSeries(unit="%")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.int64_value)

        logging.info(
            "Returning %d Disk - utilization",
            len(results.values),
        )

        return results

    def load_disk_write_bytes(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/disk/write_bytes_count"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Disk - Write Bytes (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Disk - Write Bytes", len(series_list))

        if not series_list:
            return TimeSeries(unit="%")

        results: TimeSeries = TimeSeries(unit="%")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.int64_value)

        logging.info(
            "Returning %d Disk - Write Bytes",
            len(results.values),
        )

        return results

    def load_disk_bytes_used_by_type(self) -> Dict[str, TimeSeries]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/disk/bytes_used_by_data_type"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Disk - bytes used by type (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Disk - bytes used by type", len(series_list))

        if not series_list:
            return {"Not Available" : TimeSeries(unit="bytes")}

        results: Dict[str, TimeSeries] = {}

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            data_type = mlabels["data_type"]
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )
            datas = results.get(data_type, TimeSeries(unit="bytes"))
            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                datas.add(dt, p.value.int64_value)
                results[data_type] = datas

        logging.info(
            "Returning %d types of Disk - bytes used by type",
            len(results),
        )

        return results

    def load_memory_quota(self) -> TimeSeries:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/memory/quota"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Memory - Quota (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Memory - Quota", len(series_list))

        if not series_list:
            return TimeSeries(unit="bytes")

        results: TimeSeries = TimeSeries(unit="bytes")

        for ts in series_list:
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )

            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                results.add(dt, p.value.int64_value)

        logging.info(
            "Returning %d Memory - Quota",
            len(results.values),
        )

        return results

    def load_memory_components(self) -> Dict[str, TimeSeries]:
        client = self._monitoring_client
        project_name = f"projects/{self.project_id}"
        metric_type = "cloudsql.googleapis.com/database/memory/components"

        start_time, end_time = self.get_start_end_time()
        request = {
            "name": project_name,
            "filter": (
                f'metric.type="{metric_type}" '
                f'AND resource.type="cloudsql_database" '
                f'AND resource.labels.database_id="{self.project_id}:{self.instance_id}" '
            ),
            "interval": {"start_time": start_time, "end_time": end_time},
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }

        logging.debug(
            "Loading Memory - components (project_id=%s, instance_id=%s, metric_type=%s)",
            self.project_id,
            self.instance_id,
            metric_type,
        )
        logging.debug("Time interval: start=%s end=%s", start_time, end_time)
        logging.debug("Cloud Monitoring filter: %s", request["filter"])

        # IMPORTANT: materialize the pager once; otherwise a second loop sees nothing.
        series_list = list(client.list_time_series(request=request))
        logging.info("Fetched %d time series for Memory - components", len(series_list))

        if not series_list:
            return {"Not Available" : TimeSeries(unit="bytes")}

        results: Dict[str, TimeSeries] = {}

        for ts in series_list:
            mlabels = dict(ts.metric.labels)
            data_type = mlabels["component"]
            points = sorted(
                ts.points,
                key=lambda p: (p.interval.end_time or p.interval.start_time)
            )
            datas = results.get(data_type, TimeSeries(unit="bytes"))
            for p in points:
                dt = p.interval.end_time or p.interval.start_time
                dt = dt.replace(second=0, microsecond=0)
                datas.add(dt, p.value.double_value)
                results[data_type] = datas

        logging.info(
            "Returning %d types of Memory - components",
            len(results),
        )

        return results



    def generate_cloudsql_metrics(self) -> CloudSQLMetrics:
        fetch_map = {
            "perquery_lock_time_metrics": self.load_perquery_lock_time,
            "perquery_latency_metrics": self.load_perquery_latency,
            "perquery_IO_time_metrics": self.load_perquery_IO_time,
            "wal_flushed_bytes_metrics": self.load_wal_flushed_bytes_count,
            "wal_inserted_bytes_metrics": self.load_wal_inserted_bytes_count,
            "psql_num_backends_by_state_metrics": self.load_psql_num_backends_by_state,
            "cpu_usage_time": self.load_cpu_usage_time,
            "cpu_utilization": self.load_cpu_utilization,
            "disk_quota": self.load_disk_quota,
            "disk_utilization": self.load_disk_utilization,
            "disk_read_bytes": self.load_disk_write_bytes,
            "disk_bytes_used_by_type": self.load_disk_bytes_used_by_type,
            "memory_quota": self.load_memory_quota,
            "memory_components": self.load_memory_components,

        }

        data = {}
        with ThreadPoolExecutor(max_workers=len(fetch_map)) as ex:
            future_to_attr = {ex.submit(fn): attr for attr, fn in fetch_map.items()}
            for fut in as_completed(future_to_attr):
                attr = future_to_attr[fut]
                data[attr] = fut.result()

        return CloudSQLMetrics(**data)


if __name__ == "__main__":
    from utils import load_db_secret_list

    db_secret = load_db_secret_list(r"C:\Users\kaiyi\Desktop\github\psql-cli\src\data\db-secrets.json")[0]
    metric = GMonitoringCollector(db_secret["project_id"], db_secret["instance_id"], 1).generate_cloudsql_metrics()
    # print(db_secret["project_id"])
    print(metric.wal_flushed_bytes_metrics.wal_flushed_bytes_count.data())
    # for item in metric.wal_flushed_bytes_metrics:
    #     print(item)
    #     print(item.database)
    #     for i in item.perquery_count.data():
    #         print(i)
    #         break
