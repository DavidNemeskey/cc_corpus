#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A wrapper script to interface with our manager app via API calls.
The manager calls this script and passes the request as cmd line arguments.
This wrapper starts the script described in the command line argument,
and when that finishes, it makes an API call back to the manager.
"""

import sys
import requests
import subprocess


def main():
    step_id = sys.argv[1]
    print(f"Launching script: {sys.argv[1:]}", flush=True)
    subprocess.run(sys.argv[2:])
    print(f"Successfully executed the script {sys.argv[2]}")
    # TODO this url shoud not be hardwired:
    url = f"http://127.0.0.1:8000/completed/{step_id}"
    requests.post(url)
    print(f"Finished running script {step_id}")


if __name__ == '__main__':
    main()
