import os
from collections.abc import Mapping

from confluent_kafka.schema_registry import SchemaRegistryClient

class Confluent():
    def __init__(self, schema_registry: bool = False):
        self.conf_params = {}
        self.schema_registry = schema_registry
        self.execute()

    def execute(self) -> None:

        self.set_bootstrap()
        self.set_security()
        self.set_sasl()
        self.set_session()

        if self.schema_registry:
            self.set_schemaRegistry()
            self.set_basic_auth()
        self.validate()

    def validate(self) -> bool:
        params_not_present = []

        for key, value in self.conf_params.items():
            if value == '' or value is None:
                params_not_present.append(key)

        if len(params_not_present) > 0:
            raise Exception(
                f"Params Error, check confluent params {' , '.join(params_not_present)}.")

    def get_credentials(self) -> dict:

        return self.conf_params

    def set_bootstrap(self) -> None:

        self.conf_params['bootstrap.servers'] = os.getenv('CONFLU_SERVER')

    def set_security(self) -> None:

        self.conf_params['security.protocol'] = 'SASL_SSL'

    def set_sasl(self) -> None:

        self.conf_params['sasl.mechanisms'] = 'PLAIN'
        self.conf_params['sasl.username'] = os.getenv('CONFLU_USERNAME')
        self.conf_params['sasl.password'] = os.getenv('CONFLU_PASSWORD')

    def set_session(self) -> None:

        self.conf_params['session.timeout.ms'] = os.getenv(
            'CONFLU_MS_TIMEOUT', 45000)

    def set_schemaRegistry(self) -> None:

        self.conf_params['schema.registry.url'] = os.getenv(
            'CONFLU_SCHEMA_REGISTRY_URL')

    def set_basic_auth(self) -> None:

        self.conf_params['basic.auth.credentials.source'] = os.getenv(
            'CONFLU_AUTH_CRED_SOURCE')
        self.conf_params['basic.auth.user.info'] = f"{os.getenv('CONFLU_SCHEMA_REGISTRY_KEY')}:{os.getenv('CONFLU_SCHEMA_REGISTRY_SECRET')}"
