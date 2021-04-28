#!/usr/bin/python3

"""
Main program to monitor urls and publish the results into Kafka
"""

import asyncio
import argparse
import logging
from collections import namedtuple
import time
import re
from abc import ABC, abstractmethod
from typing import List
import json

import aiohttp
import kafka

from config import MonitorConfig, MonitorConfigError

# Data structure to collect data that is published
MonResult = namedtuple('MonResult', 'time url status duration found')

class Publisher(ABC):
    """ Abstract class to publish monitoring results """

    @abstractmethod
    def publish(self, resuls: List[MonResult]):
        """ Publishes the results in some way """

class KafkaPublisher(Publisher):
    """ A publisher that publishes into Kafka """

    def __init__(self, config):
        """ Constructor """

        # producer config - copy the original
        pconfig = dict(config.publish)
        pconfig["value_serializer"] = lambda m: json.dumps(m._asdict()).encode('ascii')

        self.producer = kafka.KafkaProducer(**pconfig)

    def publish(self, results: List[MonResult]):
        """ Publishes the list of results into Kafka """

        for res in results:
            logging.debug(f"Publishing {res}")
            self.producer.send("monitor-data", res)

        self.producer.flush()


# Data structure to store the results of a probe
ProbeResult = namedtuple('ProbeResult', 'url status endtime found')

class Probe:
    """
    Class to implement a "probe", which is a single check of an url
    """

    url: str
    timeout: int
    pattern: re.Pattern

    def __init__(self, url, timeout=None, pattern=None):
        """ Constructor """

        self.url = url
        self.timeout = timeout
        self.pattern = re.compile(pattern) if pattern else None

    async def run(self, session):
        """ Runs the probe: executes the check and records the results """

        try:
            async with session.get(self.url) as resp:
                pattern_found = None
                if self.pattern:
                    # Check if the pattern specified can be found in the
                    # response body
                    try:
                        body = await resp.text()
                        pattern_found = self.pattern.search(body) is not None

                    except Exception as exc:
                        # TODO: more specific exception class above would be better
                        logging.error("Failed fetch body of {self.url}: {exc}")

                return ProbeResult(url=self.url, status=resp.status,
                                   endtime=time.time(), found=pattern_found)
        except Exception as exc:
            # TODO: more specific exception class above would be better
            logging.debug(f"Failed fetching {self.url}: {exc}")
            return ProbeResult(url=self.url, status='ERR', endtime=time.time(), found=None)


class Monitor:
    """ Class to implement the "business logic """

    config: MonitorConfig
    publisher: Publisher

    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher


    async def run(self):
        """
        Main "business logic"
        Periodically runs the probes and publishes the results
        """

        logging.info("Running monitors")

        probes = []
        for probe in self.config.probes:
            try:
                url = probe["url"]
            except KeyError:
                logging.error("Probe url missing")
                continue

            probes.append(Probe(url, self.config.timeout, probe.get("pattern")))

        async with aiohttp.ClientSession() as session:

            while True:
                # Main loop
                logging.debug("Probe loop start")

                tasks = [p.run(session) for p in probes]

                starttime = time.time()
                results = await asyncio.gather(*tasks)
                logging.debug(f"Results: {results}")
                endtime = time.time()
                elapsed = endtime-starttime
                logging.debug(f"Time elapsed: {elapsed:.2f}s")

                mon_results = [
                    MonResult(time=starttime, url=r.url, status=r.status,
                              duration=endtime-r.endtime, found=r.found)
                    for r in results
                ]

                self.publisher.publish(mon_results)

                remaining = self.config.period + starttime - time.time()
                if remaining > 0:
                    logging.debug(f"Sleeping {remaining:.2f}s")
                    await asyncio.sleep(remaining)
                else:
                    logging.error("Probing took more time than the period")

        logging.info("The end")


def get_args():
    """ Processes the command line arguments """

    ap = argparse.ArgumentParser()
    ap.add_argument('--debug', action="store_true", help="Enable debug logging")
    ap.add_argument('--config', help="Name of config file")

    return ap.parse_args()


async def main():
    """ Main """

    # processing args
    args = get_args()

    # setting up logger
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    logging.getLogger("chardet").setLevel(logging.CRITICAL)
    logging.getLogger("kafka").setLevel(logging.ERROR)

    logging.debug(f"args: {args}")

    # loading config
    try:
        config = MonitorConfig(args.config)
    except MonitorConfigError as exc:
        raise SystemExit(f"Failed loading config: {exc}")

    if not config.probes:
        raise SystemExit("No probes, nothing to do, exiting")

    # creating publiser+monitor and running them
    publisher = KafkaPublisher(config)
    monitor = Monitor(config, publisher)
    await monitor.run()

if __name__ == '__main__':
    asyncio.run(main())
