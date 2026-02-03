import os
import sys
import click
import json
import logging
from pathlib import Path
import questionary
from datetime import datetime, timezone, timedelta

import config as config
from entry import analysis_entry
from utils import ensure_adc_login, load_db_secret_list, parse_utc_minute
from cloudsql_postgres import CloudSQLPostgres

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
def test():
    click.echo("Test start")


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('project-id', type=str)
@click.argument('instance-id', type=str)
@click.argument('output_dir', type=str)
@click.option("--start-time", type=str, callback=lambda ctx, p, v: parse_utc_minute(v),
              help="UTC time: YYYY-MM-DDTHH:MM (no seconds), e.g. 2026-01-29T10:15")
@click.option("--end-time", type=str, callback=lambda ctx, p, v: parse_utc_minute(v),
              help="UTC time: YYYY-MM-DDTHH:MM (no seconds), e.g. 2026-01-29T12:15")
@click.option("--duration-hours", type=int,
              help="Duration in whole hours")
@click.option("--safe/--no-safe", default=True)
def generate(project_id, instance_id, output_dir, start_time, end_time, duration_hours, safe):
    """
    Generate Hotspots report directly. Please make sure you have run at least once command 'connect-db'

    Must Need Arguments:

        PROJECT_ID: e.g. my-analytics-prod
    """
    if not safe:
        ensure_adc_login()

    provided = [start_time is not None, end_time is not None, duration_hours is not None]
    if sum(provided) != 2:
        logging.error("Provide exactly TWO of: --start-time, --end-time, --duration-hours")
        raise click.UsageError(
            "Provide exactly TWO of: --start-time, --end-time, --duration-hours"
        )

    output_dir_path = Path(output_dir)
    if output_dir_path.exists() and output_dir_path.is_dir():
        pass
    else:
        logging.error(f"Output directory {output_dir_path} does not exist or is not a directory")
        raise click.UsageError(
            "The output directory does not exist or is not a directory"
        )

    duration_hours = duration_hours if duration_hours is not None else 0
    analysis_entry(project_id, instance_id, output_dir, start_time, end_time, duration_hours)
    pass


# @click.command(context_settings=CONTEXT_SETTINGS)
# @click.option("--provider", type=click.Choice(['Google Cloud', 'others']), default=None)
# def connect_db(provider):
#     if provider is None:
#         provider = questionary.select(
#             "Please select the hyperscaler:",
#             choices=['Google Cloud', 'others']
#         ).ask()
#
#     if provider == "Google Cloud":
#         ensure_adc_login()
#
#     mother_dir = os.path.dirname(os.path.abspath(__file__))
#     db_secret_path = os.path.join(mother_dir, "data", "db-secrets.json")
#     saved_db_secrets = load_db_secret_list(db_secret_path)
#     if len(saved_db_secrets) > 0:
#         # Build choice labels → actual objects mapping
#         choices = []
#         mapping = {}
#         for item in saved_db_secrets:
#             choices.append(item["instance_connection_name"])
#             mapping[item["instance_connection_name"]] = item
#         choices.insert(0, "Create new connection")
#         mapping["Create new connection"] = None
#
#         selected_label = questionary.select(
#             "Select a database configuration:",
#             choices=choices
#         ).ask()
#
#         if selected_label is None:
#             raise click.Abort()
#         elif selected_label == "Create new connection":
#             project_id = questionary.text(
#                 "GCP Project ID (e.g. my-analytics-prod):",
#                 default="",
#                 validate=lambda s: True if s.strip() else "Project ID cannot be empty.",
#             ).ask()
#             database_id = questionary.text(
#                 "Database/Instance ID (e.g. pg-main-01 or my-cloudsql-instance):",
#                 default="",
#                 validate=lambda s: True if s.strip() else "Database/Instance ID cannot be empty.",
#             ).ask()
#             region = questionary.text(
#                 "Region (e.g. us-central1):",
#                 default="",
#                 validate=lambda s: True if s.strip() else "Region cannot be empty.",
#             ).ask()
#             user = questionary.text(
#                 "DB username (e.g. app_user):",
#                 default="",
#                 validate=lambda s: True if s.strip() else "Username cannot be empty.",
#             ).ask()
#
#             password = questionary.password(
#                 "DB password (input hidden):",
#                 validate=lambda s: True if s.strip() else "Password cannot be empty.",
#             ).ask()
#
#             database = questionary.text(
#                 "Database name (e.g. postgres or appdb):",
#                 default="",
#                 validate=lambda s: True if s.strip() else "Database name cannot be empty.",
#             ).ask()
#
#             instance_connection_name = project_id + ":" + region + ":" + database_id
#             database_id = project_id + ":" + database_id
#
#             cfg = CloudSQLPostgresConfig(
#                 instance_connection_name=instance_connection_name,
#                 user=user,
#                 password=password,
#                 database=database,
#             )
#
#             status, err_mes = CloudSQLPostgres(cfg).check_reachable()
#             if status:
#                 click.secho(f"Successful! Currently login into\n{instance_connection_name}", fg="green")
#                 logging.info(f"Successfully logged in into {instance_connection_name}")
#
#                 saved_db_secrets.append(
#                     {"project_id": project_id, "region": region, "database_id": database_id, "username": user,
#                      "password": password, "instance_connection_name": instance_connection_name, "database": database})
#                 with open(db_secret_path, "w", encoding="utf-8") as f:
#                     json.dump(saved_db_secrets, f, indent=2)
#
#                 config.project_id = project_id
#                 config.region = region
#                 config.database_id = database_id
#                 config.username = user
#                 config.password = password
#                 config.instance_connection_name = instance_connection_name
#                 config.database = database
#
#                 logging.info({"project_id": project_id, "region": region, "database_id": database_id, "username": user,
#                               "password": password, "instance_connection_name": instance_connection_name,
#                               "database": database})
#             else:
#                 click.secho(f"Failed to login into {instance_connection_name}", fg="red")
#                 click.secho(f"Error: {err_mes}", fg="red")
#                 logging.error(f"Failed to login into {instance_connection_name}", exc_info=True)
#                 logging.error(f"Error: {err_mes}", exc_info=True)
#                 sys.exit(1)
#         else:
#             cfg = CloudSQLPostgresConfig(
#                 instance_connection_name=mapping[selected_label].get("instance_connection_name"),
#                 user=mapping[selected_label].get("username"),
#                 password=mapping[selected_label].get("password"),
#                 database=mapping[selected_label].get("database"),
#             )
#
#             status, err_mes = CloudSQLPostgres(cfg).check_reachable()
#             if status:
#                 config.project_id = mapping[selected_label].get("project_id")
#                 config.region = mapping[selected_label].get("region")
#                 config.database_id = mapping[selected_label].get("database_id")
#                 config.username = mapping[selected_label].get("username")
#                 config.password = mapping[selected_label].get("password")
#                 config.instance_connection_name = mapping[selected_label].get("instance_connection_name")
#                 config.database = mapping[selected_label].get("database")
#                 click.secho(f"Successful! Currently login into\n{config.instance_connection_name}", fg="green")
#                 logging.info(f"Successfully logged in into {config.instance_connection_name}")
#             else:
#                 click.secho(f"Failed to login into {config.instance_connection_name}", fg="red")
#                 click.secho(f"Error: {err_mes}", fg="red")
#                 logging.error(f"Failed to login into {config.instance_connection_name}", exc_info=True)
#                 logging.error(f"Error: {err_mes}", exc_info=True)
#                 sys.exit(1)
#
#     if len(saved_db_secrets) == 0:
#         project_id = questionary.text(
#             "GCP Project ID (e.g. my-analytics-prod):",
#             default="",
#             validate=lambda s: True if s.strip() else "Project ID cannot be empty.",
#         ).ask()
#         database_id = questionary.text(
#             "Database/Instance ID (e.g. pg-main-01 or my-cloudsql-instance):",
#             default="",
#             validate=lambda s: True if s.strip() else "Database/Instance ID cannot be empty.",
#         ).ask()
#         region = questionary.text(
#             "Region (e.g. us-central1):",
#             default="",
#             validate=lambda s: True if s.strip() else "Region cannot be empty.",
#         ).ask()
#         user = questionary.text(
#             "DB username (e.g. app_user):",
#             default="",
#             validate=lambda s: True if s.strip() else "Username cannot be empty.",
#         ).ask()
#
#         password = questionary.password(
#             "DB password (input hidden):",
#             validate=lambda s: True if s.strip() else "Password cannot be empty.",
#         ).ask()
#
#         database = questionary.text(
#             "Database name (e.g. postgres or appdb):",
#             default="",
#             validate=lambda s: True if s.strip() else "Database name cannot be empty.",
#         ).ask()
#
#         instance_connection_name = project_id + ":" + region + ":" + database_id
#         database_id = project_id + ":" + database_id
#
#         cfg = CloudSQLPostgresConfig(
#             instance_connection_name=instance_connection_name,
#             user=user,
#             password=password,
#             database=database,
#         )
#
#         status, err_mes = CloudSQLPostgres(cfg).check_reachable()
#         if status:
#             click.secho(f"Successful! Currently login into\n{instance_connection_name}", fg="green")
#             logging.info(f"Successfully logged in into {instance_connection_name}")
#
#             saved_db_secrets.append(
#                 {"project_id": project_id, "region": region, "database_id": database_id, "username": user,
#                  "password": password, "instance_connection_name": instance_connection_name, "database": database})
#             with open(db_secret_path, "w", encoding="utf-8") as f:
#                 json.dump(saved_db_secrets, f, indent=2)
#
#             config.project_id = project_id
#             config.region = region
#             config.database_id = database_id
#             config.username = user
#             config.password = password
#             config.instance_connection_name = instance_connection_name
#             config.database = database
#
#             logging.info({"project_id": project_id, "region": region, "database_id": database_id, "username": user,
#                           "password": password, "instance_connection_name": instance_connection_name,
#                           "database": database})
#         else:
#             click.secho(f"Failed to login into {instance_connection_name}", fg="red")
#             click.secho(f"Error: {err_mes}", fg="red")
#             logging.error(f"Failed to login into {instance_connection_name}", exc_info=True)
#             logging.error(f"Error: {err_mes}", exc_info=True)
#             sys.exit(1)
#
#         # todo: connect entry.py with config.py


@click.version_option(version=config.VERSION, prog_name='PostgreSQL Hotspots')
@click.group(context_settings=CONTEXT_SETTINGS,
             help=f'''
                      PostgreSQL Hotspots {config.VERSION}
                      
                      Performance troubleshooting tool for PostgreSQL...
                      
                      Three sentences to describe this tool:
                      1. Yeah\n
                      2. Yeahh\n
                      3. Yeahhh\n
                                                  
                    ''')
def cli():
    if config.DEBUG:
        logging.basicConfig(filename='psql-cli.log',
                            format="%(asctime)s.%(msecs)03d | %(levelname)s | %(threadName)s | %(message)s",
                            datefmt="%Y-%m-%d | %H:%M:%S",
                            encoding="utf-8",
                            level=logging.DEBUG)
    else:
        logging.basicConfig(filename='psql-cli.log',
                            format="%(asctime)s.%(msecs)03d | %(levelname)s | %(threadName)s | %(message)s",
                            datefmt="%Y-%m-%d | %H:%M:%S",
                            encoding="utf-8",
                            level=logging.INFO)

    logging.info(f" ***** PostgreSQL Hotspots {config.VERSION} starts *****")
    pass


cli.add_command(test)
cli.add_command(generate)
# cli.add_command(connect_db)


def main():
    cli()


if __name__ == '__main__':
    main()
