#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A wrapper script to interface with our manager app via API calls.
The manager calls this script and passes the request as cmd line arguments.
This wrapper starts the script described in the command line argument,
and when that finishes, it makes an API call back to the manager.
"""

from argparse import ArgumentParser
import logging
import requests
import subprocess


def parse_arguments():
    """Returns the listed and the additional parameters"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("manager_logfile",
                        help="The log file for the api wrapper script.")
    parser.add_argument("app_url",
                        help="The address of the app for callback.")
    parser.add_argument("step_id", help="The DB record ID of this step.")
    parser.add_argument("script_file", help="The script file for execution.")
    return parser.parse_known_args()


def main():
    args, extra_args = parse_arguments()
    step_id = args.step_id
    logging.basicConfig(
        filename=args.manager_logfile,
        filemode='a',
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s',
        level=logging.INFO
    )

    logging.info(f"Script {args.script_file} "
                 f"(id: {step_id}) - Starting with params: {extra_args}")
    results = subprocess.run([args.script_file] + extra_args)
    if results.returncode == 0:
        logging.info(f"Script {args.script_file} "
                     f"(id: {step_id}) - Successfully executed")
        success_url = f"{args.app_url}completed/{step_id}"
        requests.post(success_url)
        logging.info(f"Finished running script {step_id}")
    else:
        logging.info(f"Script {args.script_file} "
                     f"(id: {step_id}) - Error code: {results.returncode}")
        failed_url = f"{args.app_url}failed/{step_id}"
        requests.post(failed_url)
        logging.info(f"Reported script failing to execute {step_id}")


if __name__ == '__main__':
    main()
