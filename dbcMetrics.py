#!/usr/bin/env python3
import json
import os
import sys
import time
import zipfile
from os import path

import pandas as pd

HELP_MESSAGE = "./dbcMetrics.py <Databricks-dbc-file>"
DEBUG_MODE = False
LANGUAGES = ["scala", "python", "sql", "r"]


def main(path_to_dbc):
    folder_name = path.splitext(path_to_dbc)[0]

    with zipfile.ZipFile(path_to_dbc, 'r') as zip_ref:
        zip_ref.extractall(folder_name)

    for root, dirs, files in os.walk(folder_name):
        sorted_files = sorted(files, key=lambda file: path.splitext(file)[0])
        for name in sorted_files:
            file_to_process = path.join(root, name)
            file_to_process_extn = path.splitext(file_to_process)[1][1:]
            if file_to_process_extn in LANGUAGES:
                log_to_console(f"\nProcessing: {file_to_process}")
                out_file = path.splitext(file_to_process)[0] + ".csv"
                with open(file_to_process) as f:
                    data = json.load(f)

                commands = data['commands']
                metrics = []
                all_commands_output = []
                for command in commands:
                    metric = DbcMetric(command['command'],
                                       command['state'],
                                       command['startTime'],
                                       command['submitTime'],
                                       command['finishTime'])
                    metrics.append([metric.command_trimmed, metric.exec_mins])
                    indiv_output = f"{metric.exec_secs: >10.2f}sec ({metric.exec_mins: >5.1f}min): " \
                        f"({metric.submit_time: >20} {metric.start_time: >20}) to {metric.finish_time: >20}: " \
                        f"{metric.state: >10}: {metric.command_trimmed}"
                    all_commands_output.append(indiv_output)
                    log_to_console(indiv_output)
                log_to_file(out_file, "w", '\n'.join(all_commands_output) + "\n\n")
                pd.set_option('display.max_colwidth', 100)
                pd.set_option('display.float_format', lambda x: '%.4f' % x)
                df = pd.DataFrame(metrics, columns=['command_trimmed', 'exec_mins'])
                df['exec_mins'] = df['exec_mins'].astype(float)
                df = df.sort_values(by=['exec_mins'], ascending=False)[['exec_mins', 'command_trimmed']]
                log_to_console(f"Pandas: {df}")
                df.to_csv(out_file, mode='a', sep="\t", float_format='%.4f', header=['exec_time_mins', 'command'])
                final_msg = f"\n\n**Total time to execute '{file_to_process}': {df['exec_mins'].sum(): >.4f} mins.**"
                log_to_file(out_file, "a", final_msg)
                print(final_msg)
    return None


class DbcMetric:
    def __init__(self, command, state, start_time, submit_time, finish_time):
        self.command = command
        self.state = state
        self.start_time = start_time
        self.submit_time = submit_time
        self.finish_time = finish_time
        self.command_trimmed = command.replace('\n', ' ').replace('\t', '')
        self.start_time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                                            time.localtime(start_time / 1000)) if start_time > 0 else '--'
        self.submit_time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                                             time.localtime(submit_time / 1000)) if submit_time > 0 else '--'
        self.finish_time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                                             time.localtime(finish_time / 1000)) if finish_time > 0 else '--'
        self.exec_secs = (finish_time - start_time) / 1000.0
        self.exec_mins = self.exec_secs / 60.0


def log_to_file(out_file, mode, event):
    with open(out_file, mode) as f:
        f.write(event)
    return None


def log_to_console(event):
    if DEBUG_MODE:
        print(event)
    return None


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"{HELP_MESSAGE}  num args = {len(sys.argv)}")
        sys.exit(1)
    print(f"Extracting Metrics from DBC archive file: {sys.argv[1]}\n")
    main(sys.argv[1])
    sys.exit(0)
