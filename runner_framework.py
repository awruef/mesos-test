#!/usr/bin/env python2.7
import argparse 
import json 
import csv
import sys

def get_tasks(data_dir):
    """
    proglist format : id,program,args
    inputlist format: filename,stdin
    """
    results = []
    proglist = csv.reader(open('{}/proglist.csv'.format(data_dir), 'r'))
    inputlist = csv.reader(open('{}/inputlist.csv'.format(data_dir), 'r'))
    taskid = 0
    first = True
    inputs = []
    programs = []
    for p in proglist:
        if first == True:
            first = False
        else:
            programs.append(p)
    first = True
    for i in inputlist:
        if first == True:
            first = False
        else:
            inputs.append(i)

    for p in programs:
        program_id = p[0]
        program = p[1]
        program_args = p[2]
        for i in inputs:
            input_filename = i[0]
            is_stdin = i[1]
            if is_stdin == 'False':
                is_stdin = False
            else:
                is_stdin = True
            results.append((taskid,program,program_args,input_filename,is_stdin))
            taskid = taskid + 1
    return results

def main(args):
    # First, read the data we need to produce the task list. 
    task_list = [] # A list of commands to run of the form (seq,command,args,input,stdin)
    task_list = get_tasks(args.data_dir)

    # Then, boot up all the Mesos crap and get us registered with the framework. 

if __name__ == '__main__':
    parser = argparse.ArgumentParser('framework')
    parser.add_argument('data_dir', type=str, help="Data directory")
    args = parser.parse_args()
    sys.exit(main(args))
