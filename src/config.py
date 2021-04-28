"""
Module to deal with the configuration file
It mainly just reads it and deals with some defaults
"""

import yaml

class MonitorConfigError(Exception):
    """ Raised in case of issues loading the config """

class MonitorConfig:
    """ Class to store config data """

    urls: list[str]
    period: int
    timeout: int
    kafka_url: str
    db: str

    def __init__(self, cfg_name=None):
        config = {}
        if cfg_name:
            with open(cfg_name) as cfg_file:
                try:
                    config = yaml.safe_load(cfg_file)
                except Exception as exc:
                    raise MonitorConfigError(f"Failed loading {cfg_name}: {exc}")

        self.probes = config.get("probes", [])
        self.period  = config.get("period", 60)
        self.timeout = config.get("timeout", 0)
        self.publish = config.get("publish", None)
        self.db = config.get("db", None)
