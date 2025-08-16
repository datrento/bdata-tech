import os
import time
from pyflink.table import TableEnvironment, EnvironmentSettings
from abc import ABC, abstractmethod

class BaseJob(ABC):
    def __init__(self, job_name):
        env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.t_env = TableEnvironment.create(env_settings)

        # Configuration for the job
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka:9093')
        self.postgres_url = os.getenv('POSTGRES_JDBC_URL', 'jdbc:postgresql://postgres:5432/price_intelligence')
        self.postgres_user = os.getenv('POSTGRES_USER', 'postgres')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.job_name = job_name
        # Unique Kafka consumer group to avoid reusing committed offsets
        default_group_id = f"price_monitoring_group_{int(time.time())}"
        self.kafka_group_id = os.getenv('FLINK_KAFKA_GROUP_ID', default_group_id)

        # Set checkpoint interval 
        self.t_env.get_config().get_configuration().set_string(
            "execution.checkpointing.interval", "10s"
        )

        # Set Flink local time zone (affects TIMESTAMP_LTZ semantics)
        flink_tz = os.getenv('FLINK_LOCAL_TIME_ZONE', 'UTC')
        self.t_env.get_config().get_configuration().set_string(
            "table.local-time-zone", flink_tz
        )

        print(f"Initialized job: {self.job_name}")
        print(f"Using Kafka bootstrap servers: {self.kafka_bootstrap_servers}")
        print(f"Using PostgreSQL URL: {self.postgres_url}")
        print(f"Flink local time zone: {flink_tz}")
        print(f"Kafka consumer group id: {self.kafka_group_id}")

    @abstractmethod
    def setup_tables(self):
        pass

    @abstractmethod
    def run(self):
        print(f"Running job: {self.job_name}")
        pass