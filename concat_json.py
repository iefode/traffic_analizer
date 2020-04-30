import os
import glob

import logging
from argparse import ArgumentParser

import json
import ijson
import csv

import pandas as pd
import random

logging.basicConfig()
logger = logging.getLogger('CLEANING APP')
logger.setLevel(logging.INFO)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-i', '--path_to_input_folder', required=True, type=str,
                        help='Path to json file from WireShark')
    parser.add_argument('-o', '--path_to_output_folder', required=False, default="", type=str,
                        help='Path to updated json file from WireShark')
    return parser.parse_args()


def extract_data(json_paths: list):
    processed_data = [[
                        "name",
                        "country",
                        "hours",
                        "minutes",
                        "weekday",
                        "ip.src",
                        "ip.dst",
                        "ip.src_host",
                        "ip.dst_host",
                        "tcp.srcport",
                        "tcp.dstport"
                    ]]
    for json_path in json_paths:
        logger.info("Processing of {}".format(json_path))
        name = os.path.split(json_path)[1].split('.')[0]
        with open(json_path, 'r') as f:
            objects = ijson.items(f, '')

            for column in objects:
                for item in column:
                    time = item["time"].split()[3].split(':')
                    country = item["time"].split()[4]
                    pass
                    # updated_item = item
                    # updated_item.update({"name": name})
                    updated_item = [
                        name,
                        country,
                        time[0],
                        time[1],
                        # item["time"],
                        item["weekday"],
                        item["ip.src"],
                        item["ip.dst"],
                        item["ip.src_host"],
                        item["ip.dst_host"],
                        item["tcp.srcport"],
                        item["tcp.dstport"]
                    ]
                    processed_data.append(updated_item)
        logger.info("Processing of {} has finished".format(json_path))
    return processed_data


if __name__ == "__main__":
    args = parse_args()

    json_files = list()
    if os.path.isdir(args.path_to_input_folder):
        logger.info("Input folder is {}".format(args.path_to_input_folder))
        json_files = glob.glob(os.path.join(args.path_to_input_folder, "*.json"))
    else:
        raise Exception("Incorrect path to the input folder!")

    data = extract_data(json_files)

    out_dir = os.path.dirname(args.path_to_input_folder) if args.path_to_output_folder == "" else args.path_to_output_folder
    out_filename = os.path.join(out_dir, "out_validation.csv")
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    with open(out_filename, 'w') as out_file:
        logger.info("FILE {} IS SAVED".format(out_filename))
        with open(out_filename, 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerows(data)
        # json.dump(data, out_file)

    logger.info("APPLICATION IS SUCCESSFUL FINISHED")