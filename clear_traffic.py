import os
import glob

import logging
from argparse import ArgumentParser

import json
import ijson

import pandas as pd
import random


IP_PROTOCOL_NAME = "ip"
TCP_PROTOCOL_NAME = "tcp"

PERCENTS = {"real": 0.05,
            "validation": 0.25,
            "learning": 0.7}

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
    processed_data = list()
    for json_path in json_paths:
        logger.info("Processing of {}".format(json_path))
        with open(json_path, 'r') as f:
            objects = ijson.items(f, '')

            for column in objects:
                for item in column:
                    source_data = item["_source"]["layers"]
                    if not (IP_PROTOCOL_NAME in source_data and TCP_PROTOCOL_NAME in source_data):
                        continue
                    ts2weekday = lambda ts: pd.Timestamp(ts, unit='s').weekday()
                    updated_item = {
                        "time": source_data["frame"]["frame.time"],
                        "weekday": ts2weekday(float(source_data["frame"]["frame.time_epoch"])),
                        "ip.src": source_data["ip"]["ip.src"],
                        "ip.dst": source_data["ip"]["ip.dst"],
                        "ip.src_host": source_data["ip"]["ip.src_host"],
                        "ip.dst_host": source_data["ip"]["ip.dst_host"],
                        "tcp.srcport": source_data["tcp"]["tcp.srcport"],
                        "tcp.dstport": source_data["tcp"]["tcp.dstport"]
                    }
                    processed_data.append(updated_item)
        logger.info("Processing of {} has finished".format(json_path))
    return processed_data


if __name__ == "__main__":
    args = parse_args()

    json_files = dict()
    if os.path.isdir(args.path_to_input_folder):
        logger.info("Input folder is {}".format(args.path_to_input_folder))
        for dir in glob.glob(os.path.join(args.path_to_input_folder, '*')):
            if os.path.isdir(dir):
                logger.info("Folder {} is added".format(dir))
                json_files.update({os.path.split(dir)[1]: glob.glob(os.path.join(dir, "*.json"))})
    else:
        raise Exception("Incorrect path to the input folder!")

    for dir, json_paths in json_files.items():
        data = extract_data(json_paths)

        random.shuffle(data)

        object_count = len(data)
        cnt = 0
        dist = dict()
        for name, p in PERCENTS.items():
            num = int(p * object_count)
            logger.info("SET_NAME IS \"{}\", NUM IS \"{}\" GENERAL COUNTER \"{}\"".format(name, num, object_count))
            dist.update({name: (cnt, cnt + num)})
            cnt += num

        pass

        for set_name, (i_start, i_end) in dist.items():
            set = data[i_start:i_end]
            out_dir = os.path.dirname(args.path_to_input_folder) if args.path_to_output_folder == "" else args.path_to_output_folder
            out_dir = os.path.join(out_dir, set_name)
            out_filename = os.path.join(out_dir, dir + ".json")
            if not os.path.exists(out_dir):
                os.mkdir(out_dir)
            with open(os.path.join(dir, out_filename), 'w') as out_file:
                logger.info("FILE {} IS SAVED".format(out_filename))
                json.dump(set, out_file)
        data.clear()

        logger.info("APPLICATION IS SUCCESSFUL FINISHED")