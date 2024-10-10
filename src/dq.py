import yaml
import logging
import importlib
from pathlib import Path

import psycopg2
from contextlib import closing
from typing import List, Tuple, Union, Optional

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


class DataQualityChecker:
    action_function_template = "{root_folder}.actions.{action_name}.action"
    comparator_function_template = "{root_folder}.comparators.{comparator}.comparator"

    def __init__(
            self,
            metric_name: str,
            prometheus_metric_prefix: str,
            sources: dict,
            attributes: dict,
            connections: dict,
            prometheus_gateway: Optional[str] = None
    ):
        self.metric_name = metric_name
        self.prometheus_metric_prefix = prometheus_metric_prefix.lower()
        self.prometheus_metric_name = metric_name.lower()
        self.sources = sources
        self.attributes = attributes
        self.connections = connections
        self.base_dir = Path(__file__).parent.resolve()
        self.prometheus_gateway = prometheus_gateway

        metric_description = DataQualityChecker.read_yml_file(
            file_path=self.base_dir.joinpath(
                'metrics_description.yml'
            )
        )[self.metric_name]
        self.action = metric_description["action"]
        self.comparator = metric_description["comparator"]
        self.prometheus_documentation = metric_description["prometheus_documentation"]

        self.logger = logging.getLogger(__name__)

    @classmethod
    def read_yml_file(cls, file_path: Union[str, Path]) -> dict:
        with open(file_path, "r") as f:
            content = yaml.safe_load(f)
        return content

    @classmethod
    def read_sql_file(cls, file_path: Union[str, Path]) -> str:
        with open(file_path, 'r') as f:
            content = f.read()
        return content

    @classmethod
    def fetchall(cls, conn_params: dict, query: str) -> List[Tuple]:
        with closing(psycopg2.connect(**conn_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
            pg_cursor.execute(query)
            records = pg_cursor.fetchall()
            return records

    def send_metric_to_prometheus(self, value: int) -> None:
        registry = CollectorRegistry()
        g = Gauge(
            name=self.prometheus_metric_name,
            documentation=self.prometheus_documentation,
            labelnames=['instance'],
            registry=registry
        )
        g.labels(instance='dq').set(value)
        push_to_gateway(
            gateway=self.prometheus_gateway,
            job=self.prometheus_metric_prefix,
            registry=registry
        )
        self.logger.info(f'Metric {self.prometheus_metric_name} with {value} value push to Prometheus')

    def check(self) -> None:
        self.logger.info(f"Metric_name: {self.metric_name}")
        self.logger.info(f"Prometheus_metric_name: {self.prometheus_metric_name}")
        self.logger.info(f"Sources: {self.sources}")
        self.logger.info(f"Attributes: {self.attributes}")

        values_to_compare = []
        for source in self.sources.keys():
            source_params = self.sources[source]
            attrs = self.attributes | source_params
            query = DataQualityChecker.read_sql_file(
                file_path=self.base_dir.joinpath(
                    f'metrics/{self.metric_name.lower()}/metric.sql'
                )
            ).format(**attrs)
            self.logger.info(f"Query: \n{query}")
            rows = DataQualityChecker.fetchall(
                conn_params=self.connections[source_params["connection"]],
                query=query
            )
            values_to_compare.append(rows)

        metric_value = importlib.import_module(
            self.action_function_template.format(
                root_folder=self.base_dir.as_posix().split('/')[-1],
                action_name=self.action
            )
        ).run(values_to_compare)
        self.logger.info(f"Metric: {metric_value}")

        if self.prometheus_gateway:
            self.send_metric_to_prometheus(value=metric_value)

        comparison_kwargs = self.attributes | {"metric": metric_value}
        comparison_result = importlib.import_module(
            self.comparator_function_template.format(
                root_folder=self.base_dir.as_posix().split('/')[-1],
                comparator=self.comparator
            )
        ).run(**comparison_kwargs)

        self.logger.info(f"Comparison_result: {comparison_result}")

        # TODO: add alerting in case when comparison_result is True
