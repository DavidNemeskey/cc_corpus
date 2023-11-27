#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A wrapper script to interface with our manager app via API calls.
The manager calls this script and passes the request as cmd line arguments.
This wrapper starts the script described in the command line argument,
and when that finishes, it makes an API call back to the manager.
"""

import logging
import sys
import requests
import subprocess


def main():
    step_id = sys.argv[2]

    logging.basicConfig(
        filename= sys.argv[1],
        filemode='a',
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s',
        level=logging.INFO
    )

    logging.info(f"Script {sys.argv[3]} (id: {step_id}) - Starting with: {sys.argv[2:]}")
    results = subprocess.run(sys.argv[3:])
    if results.returncode == 0:
        logging.info(f"Script {sys.argv[3]} (id: {step_id}) - Successfully executed")
        # TODO this url shoud not be hardwired:
        success_url = f"http://127.0.0.1:8000/completed/{step_id}"
        requests.post(success_url)
        logging.info(f"Finished running script {step_id}")
    else:
        logging.info(f"Script {sys.argv[3]} (id: {step_id}) - Error code: {results.returncode}")
        # TODO this url shoud not be hardwired:
        failed_url = f"http://127.0.0.1:8000/failed/{step_id}"
        requests.post(failed_url)
        logging.info(f"Reported script failing to execute {step_id}")


if __name__ == '__main__':
    main()
