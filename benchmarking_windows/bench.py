#!/usr/bin/env python3
# This script is used to benchmark the catalog server.
# It will start the catalog server, seed the catalog with some namespaces and tables, and use vegeta to stress test the server.
# vegeta: https://github.com/tsenart/vegeta
# Install on mac: brew install vegeta

import subprocess as sp
import time
import signal
import sys
import requests
import argparse
import string
import random


def get_random_str(length=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def run(cmd, note, bg=False, out=None):
    print(f"{note.ljust(48)}...", end=" ", flush=True)
    try:
        res = None
        if out:
            with open(out, "a") as f:
                if bg:
                    res = sp.Popen(cmd, shell=True, stdout=f, stderr=f)
                else:
                    sp.run(cmd, shell=True, check=True,
                           stdout=f, stderr=f)
        else:
            if bg:
                res = sp.Popen(cmd,   stdout=sp.DEVNULL, stderr=sp.DEVNULL)
            else:
                sp.run(cmd, shell=True, check=True,
                       stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        print("DONE!")
        return res
    except sp.CalledProcessError as e:
        print("FAIL!")
        print("Error:", e)


TEST_ROOT_DIR = "test"
DEFAULT_BINARY_NAME = "catalog2"
DEFAULT_DB_ROOT_DIR = f"{TEST_ROOT_DIR}/db"
DEFAULT_BASE_URL = "http://127.0.0.1:8000/v1/"
DEFAULT_NAMESPACE_NUM = 1
DEFAULT_TABLE_NUM = 1
DEFAULT_RATE = 8

parser = argparse.ArgumentParser(description="Benchmark.")
parser.add_argument("-b", "--binary_name", type=str,
                    default=DEFAULT_BINARY_NAME, help="Name of the catalog binary.")
parser.add_argument("-d", "--db_root", type=str,
                    default=DEFAULT_DB_ROOT_DIR, help="Root directory for the database.")
parser.add_argument("-u", "--base_url", type=str,
                    default=DEFAULT_BASE_URL, help="Base URL for catalog server.")
parser.add_argument("-n", "--namespace_num", type=int,
                    default=DEFAULT_NAMESPACE_NUM, help="The number of namespace to seed in catalog.")
parser.add_argument("-t", "--table_num", type=int,
                    default=DEFAULT_TABLE_NUM, help="The number of table to seed in catalog.")
parser.add_argument("-r", "--rate", type=int,
                    default=DEFAULT_RATE, help="Request rate.")
parser.add_argument("-p", "--plot", action="store_true",
                    default=False, help="Generate a plot of this benchmark.")
args = parser.parse_args()


CATALOG_LOG = f"{TEST_ROOT_DIR}/catalog.log"

# build catalog in release mode
run(f"rm -rf {TEST_ROOT_DIR} && mkdir {TEST_ROOT_DIR}",
    note="initializing test dir")
run(f"cargo build --release && cp target/release/{args.binary_name}.exe {TEST_ROOT_DIR}/{args.binary_name}.exe",
    note="building catalog in release mode")
catalog_server = run(f".\{TEST_ROOT_DIR}\{args.binary_name}.exe --db-root {args.db_root}",
                     note="starting catalog server", bg=True, out=CATALOG_LOG)
print("Waiting for catalog server to start...")
time.sleep(1)

# seeding the catalog, uniformly distribute tables to namespaces
print(f"Seeding namespaces and tables...")
NAMESPACE_ENDPOINT = "namespaces"
TABLE_ENDPOINT = "tables"
namespaces = []
table_per_namespace = args.table_num // args.namespace_num
i = 0
for i in range(args.namespace_num):
    namespace = get_random_str(32)
    tables = []
    for j in range(table_per_namespace):
        tables.append(get_random_str(32))
    namespaces.append({'name': namespace, 'tables': tables})
    # create namespace
    response = requests.post(f"{args.base_url}/{NAMESPACE_ENDPOINT}",
                             json={'name': [str(namespace)], "properties": {'foo': 'bar'}})
    assert True, f"Failed to create namespace {namespace}"

    # crate tables
    for table in tables:
        response = requests.post(
            f"{args.base_url}/{NAMESPACE_ENDPOINT}/{namespace}/{TABLE_ENDPOINT}",
            json={'name': table}
        )
        assert response.status_code == 201, f"Failed to create Table {table}"
        print(i)
        i += 1

print(f"Seeded {len(namespaces)} namespaces and {len(namespaces) * table_per_namespace} tables.")

# test begins
# 1. single endpoint stress test
namespace = namespaces[0]
table = namespace['tables'][0]
targets = {
    "get_table": f"{args.base_url}/{NAMESPACE_ENDPOINT}/{namespace['name']}/{TABLE_ENDPOINT}/{table}",
    "list_table": f"{args.base_url}/{NAMESPACE_ENDPOINT}/{namespace['name']}/{TABLE_ENDPOINT}",
    "get_namespace": f"{args.base_url}/{NAMESPACE_ENDPOINT}/{namespace['name']}",
    "list_namespace": f"{args.base_url}/{NAMESPACE_ENDPOINT}"
}

for name, target in targets.items():
    STATISTIC_FILE = rf"{TEST_ROOT_DIR}\results_{name}.bin"
    attack_cmd = f"echo GET {target} | vegeta attack -rate={args.rate} -duration=10s > {STATISTIC_FILE}"

    with open(rf"{TEST_ROOT_DIR}\vegeta_{name}.log", "w", encoding='utf-8') as f:
        sp.run(attack_cmd, shell=True, stdout=f, stderr=sp.STDOUT)
        report_cmd = f"vegeta report < {STATISTIC_FILE}"
        sp.run(report_cmd, shell=True, stdout=f, stderr=sp.STDOUT)

    if args.plot:
        PLOT_FILE = rf"{TEST_ROOT_DIR}\plot_{name}.html"
        plot_cmd = f"cat {STATISTIC_FILE} | vegeta plot > {PLOT_FILE}"
        sp.run(plot_cmd, shell=True)
# ... more?
# 2. random endpoint stress test
# Define the file path
# Write the URLs to the file
PATH_TARGET_FILE = rf"{TEST_ROOT_DIR}\targets.txt"
with open(PATH_TARGET_FILE, "w", newline="\r\n") as file:
    for i in range(len(namespaces)):
        random_namespace = random.choice(namespaces)
        random_table = random.choice(random_namespace['tables'])
        # Generate request URL
        target = f"{args.base_url}/{NAMESPACE_ENDPOINT}/{random_namespace['name']}/{TABLE_ENDPOINT}/{random_table}"
        request_url = f"GET {target}"
        file.write(request_url + "\r\n")
print(f"URLs have been written to {PATH_TARGET_FILE}")

STATISTIC_FILE = rf"{TEST_ROOT_DIR}\results_random.bin"
attack_cmd = f"vegeta attack -targets={PATH_TARGET_FILE} -rate={args.rate} -duration=60s > {STATISTIC_FILE}"
report_cmd = f"type {STATISTIC_FILE} | vegeta report"

with open(rf"{TEST_ROOT_DIR}\vegeta_random.log", "w", encoding='utf-8') as f:
    sp.run(attack_cmd, shell=True, stdout=f, stderr=sp.STDOUT)
    sp.run(report_cmd, shell=True, stdout=f, stderr=sp.STDOUT)

if args.plot:
    PLOT_FILE = rf"{TEST_ROOT_DIR}\plot_random.html"
    plot_cmd = f"type {STATISTIC_FILE} | vegeta plot > {PLOT_FILE}"
    sp.run(plot_cmd, shell=True)
# clean up
catalog_server.send_signal(signal.SIGINT)
