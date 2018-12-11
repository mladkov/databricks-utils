#!/usr/bin/env python3

import sys
import json
import time
import numpy as np
import pandas as pd

HELP = "./dbcMetrics.py <Databricks-dbc-file>"

def main(args):
    print("Extracting Metrics from DBC archive file: {}".format(args[1]))

    fileName    = args[1]
    outFileName = fileName[:fileName.rindex(".")] + ".csv"
    print("DBC Filename     : {}".format(fileName))
    print("Metrics Filename : {}".format(outFileName))

    with open(fileName) as f:
        data = json.load(f)

    print("Name: {}".format(data['name']))
    commands = data['commands']
    metrics = list()
    for c in commands:
        d = DBCMetric(c['command'], c['state'], c['startTime'], c['submitTime'], c['finishTime'])
        print("{executeSeconds: >10.2f}sec ({executeMinutes: >5.1f}min): {startTime: >20} ({submitTime: >20}) to {finishTime: >20}: {state: >10}: {command}".format(startTime = d.startTimeStr, submitTime = d.submitTimeStr, finishTime = d.finishTimeStr, executeSeconds = d.executeSeconds, executeMinutes = d.executeMinutes, state = d.state, command = d.cleanCommand[:150]))
        metrics.append([d.command, float(d.executeMinutes)])
    pd.set_option('display.max_colwidth', 100)
    metricArray = np.array(metrics)
    dt = np.dtype({'names':['command','execute_minutes'], 'formats':[np.generic, np.float]})
    p = pd.DataFrame(metricArray,columns=['command','execute_minutes'])
    p1 = p.astype({'execute_minutes':np.float})
    #p = pd.DataFrame(metricArray,columns=['command','execute_minutes'],dtype=[('execute_minutes', np.float),('command', np.generic)])
    print("Pandas: {}".format(p1.sort_values(by=['execute_minutes'], ascending=False)[['execute_minutes', 'command']]))
    print("Sum (minutes): {}".format(p1['execute_minutes'].sum()))

class DBCMetric:
    def __init__(self, command, state, startTime, submitTime, finishTime):
        self.command = command
        self.state = state
        self.startTime = startTime
        self.submitTime = submitTime
        self.finishTime = finishTime
        self.cleanCommand = command.replace('\n', ' ')
        self.startTimeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startTime/1000)) if startTime > 0 else '--'
        self.submitTimeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(submitTime/1000)) if submitTime > 0 else '--'
        self.finishTimeStr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(finishTime/1000)) if finishTime > 0 else '--'
        self.executeSeconds = (finishTime - startTime) / 1000.0
        self.executeMinutes = self.executeSeconds / 60

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(HELP + " num args = {}".format(len(sys.argv)))
        sys.exit(1)
    main(sys.argv)
    sys.exit(0)
