from pathlib import Path
import shutil

from figure_logic.sql_related import *
from figure_logic.wal_related import *
from figure_logic.network_related import *
from figure_logic.disk_related import *
from figure_logic.general_related import *
from metrics import CloudSQLMetrics
from hotspots_report import HotspotsReport
from cloudsql_postgres import CloudSQLPostgres
import config as config

from g_monitoring_collector import GMonitoringCollector


# MetricSpec + MetricsLoaderMP come from the ProcessPool version we wrote earlier
# specs = [
#     MetricSpec("cpu/utilization", "cloudsql.googleapis.com/database/cpu/utilization"),
#     MetricSpec("cpu/reserved_cores", "cloudsql.googleapis.com/database/cpu/reserved_cores"),
#     MetricSpec("cpu/usage_time", "cloudsql.googleapis.com/database/cpu/usage_time"),
#     MetricSpec("disk/quota", "cloudsql.googleapis.com/database/disk/quota"),
#     MetricSpec("disk/utilization", "cloudsql.googleapis.com/database/disk/utilization"),
#     MetricSpec("disk/read_bytes", "cloudsql.googleapis.com/database/disk/read_bytes_count"),
#     MetricSpec("disk/read_ops", "cloudsql.googleapis.com/database/disk/read_ops_count"),
#     MetricSpec("disk/write_bytes", "cloudsql.googleapis.com/database/disk/write_bytes_count"),
#     MetricSpec("disk/write_ops", "cloudsql.googleapis.com/database/disk/write_ops_count"),
#     MetricSpec("disk/bytes_used", "cloudsql.googleapis.com/database/disk/bytes_used"),
#     MetricSpec("disk/bytes_used_by_type", "cloudsql.googleapis.com/database/disk/bytes_used_by_data_type")
# ]

def analysis_entry(project_id, instance_id, output_dir, start_time, end_time, duration_hours):
    # start_time = datetime(2026, 1, 29, 20, 30, 0, tzinfo=timezone.utc)
    time_fmt = "%Y-%m-%d %H_%M UTC"

    collector = GMonitoringCollector(project_id, instance_id, start_time=start_time, end_time=end_time,
                                   duration_hours=duration_hours)

    metrics = collector.generate_cloudsql_metrics()
    start_time, end_time = collector.get_start_end_time()

    parent_dir_path = Path(output_dir)
    report_dir_path = parent_dir_path / f"PostgreSQL_Hotspots_{start_time.strftime(time_fmt)}_{end_time.strftime(time_fmt)}"
    if report_dir_path.exists():
        shutil.rmtree(report_dir_path)
    report_dir_path.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR_PATH = report_dir_path

    report = HotspotsReport(
        version=config.VERSION,
        report_title_base="Google Cloud Monitoring Edition",
        system_info={
            "timeframe": f"{start_time.strftime(time_fmt)} ~ {end_time.strftime(time_fmt)}",
            "system": f"{project_id}:{instance_id}",
        },
    )

    # --- Start Analysis ---

    report.add_figures([
        {
            "category": "General",
            "title": "Database History",
            "figure_html": HotspotsReport.plotly_fragment(general_database_overview(metrics)),
            "notes": ["High CPU Usage is normal and acceptable as the database might parallel tasks for efficiency.",
                      "If High CPU Usage is suspicious, comparing the period with IO performance/waiting time/Savepoints, etc, are recommended."],
        },
        {
            "category": "SQL",
            "title": "SQL with Most Latency Time",
            "figure_html": HotspotsReport.plotly_fragment(sql_perquery_latency_metrics(metrics)),
            "notes": ["[[Open full analysis|general_database_overview.txt]]",
                      "general_database_overview.txt",
                      "One commit should <= 10 ms"],
        },
        {
            "category": "SQL",
            "title": "SQL with Most IO Wait Time(Meta)",
            "figure_html": HotspotsReport.plotly_fragment(sql_perquery_io_time_metrics(metrics)),
            "notes": ["Often the symptom from another long-running SQL query holding the lock"],
        },
        {
            "category": "SQL",
            "title": "SQL with Most Lock Wait Time(Meta)",
            "figure_html": HotspotsReport.plotly_fragment(sql_perquery_lock_time_metrics(metrics)),
            "notes": ["Often the symptom from another long-running SQL query holding the lock"],
        },
        {
            "category": "Disk",
            "title": "Disk Overview(Meta)",
            "figure_html": HotspotsReport.plotly_fragment(disk_io_and_usage_timeseries(metrics)),
            "notes": ["No Note at this stage"],
        },
        {
            "category": "Network",
            "title": "Network Overview",
            "figure_html": HotspotsReport.plotly_fragment(network_overview(metrics)),
            "notes": ["No Note at this stage"],
        },
        {
            "category": "WAL",
            "title": "WAL History(Beta)",
            "figure_html": HotspotsReport.plotly_fragment(wal_overview(metrics)),
            "notes": ["No Note at this stage"],
        },
    ])

    # report.add_figures([
    #     {
    #         "category": "CPU",
    #         "title": "CPU Usage",
    #         "figure_html": HotspotsReport.plotly_fragment(export_cloudsql_cpu_plot_html(metrics)),
    #         "notes": ["High CPU Usage is normal and acceptable as the database might parallel tasks for efficiency.",
    #                   "If High CPU Usage is suspicious, comparing the period with IO performance/waiting time/Savepoints, etc, are recommended."],
    #     },
    #     {
    #         "category": "SQL",
    #         "title": "SQL consumption Overview",
    #         "figure_html": HotspotsReport.plotly_fragment(sql_consumption_overview(metrics)),
    #         "notes": ["Average Execution time under 10 ms is healthy", "Above 20ms should be investigated; 100ms indicates something goes wrong"],
    #     },
    #     {
    #         "category": "SQL",
    #         "title": "WAL-heavy queries",
    #         "figure_html": HotspotsReport.plotly_fragment(sql_wal_heavy_job(metrics)),
    #         "notes": ["Filtered to top 20 statements.", "CPU sampled every 5 seconds."],
    #     },
    #     {
    #         "category": "Disk",
    #         "title": "Cloud SQL Disk Usage",
    #         "figure_html": HotspotsReport.plotly_fragment(disk_usage_pie_overview(metrics)),
    #         "notes": ["Filtered to top 20 statements.", "CPU sampled every 5 seconds."],
    #     },
    #     {
    #         "category": "Disk",
    #         "title": "Cloud SQL Disk Usage & IO Overview",
    #         "figure_html": HotspotsReport.plotly_fragment(disk_io_and_usage_timeseries(metrics)),
    #         "notes": ["Filtered to top 20 statements.", "CPU sampled every 5 seconds."],
    #     },
    #
    # ])

    report.render(f"report_{start_time.strftime(time_fmt)}_{end_time.strftime(time_fmt)}.html")
    print("Wrote postgres_hotspots_report.html (offline, single file).")


if __name__ == "__main__":
    analysis_entry()
