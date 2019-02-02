#!/usr/bin/env python

import sys, subprocess, logging, itertools, argparse

log = logging.getLogger(__name__)

def processDirectoryIntoUnits(sourceDir, unitSize, dirAllocations, unitsSpaceAvailable):
    log.info("Calculating directory sizes for '%s'", sourceDir)
    startIdx = len(dirAllocations)
    process = subprocess.Popen("hadoop fs -du -x '{0}'".format(sourceDir), stdout=subprocess.PIPE, shell=True)
    dirAllocations += [{
        'path': line.split(None, 2)[2].rstrip(),
        'size': long(line.split()[0]),
        'unit': 0
    } for line in process.stdout]
    for dirIdx in range(startIdx, len(dirAllocations)):
        if dirAllocations[dirIdx]["size"] > unitSize:
            # Recurse down the path
            processDirectoryIntoUnits(dirAllocations[dirIdx]["path"], unitSize, dirAllocations, unitsSpaceAvailable)
        else:
            for unitIdx in range(0, len(unitsSpaceAvailable)):
                if (unitsSpaceAvailable[unitIdx] >= dirAllocations[dirIdx]["size"]):
                    dirAllocations[dirIdx]["unit"] = unitIdx + 1
                    unitsSpaceAvailable[unitIdx] -= dirAllocations[dirIdx]["size"]
                    break
            else:
                # Allocate new unit
                unitsSpaceAvailable += [unitSize]
                unitIdx = len(unitsSpaceAvailable) - 1
                dirAllocations[dirIdx]["unit"] = unitIdx + 1
                unitsSpaceAvailable[unitIdx] -= dirAllocations[dirIdx]["size"]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Calculate filelist of HDFS contents into Databox sized blocks")
    parser.add_argument('-s', '--databox-size', default=3469036354L, type=long, help="The size of each Databox in B.")
    parser.add_argument('-p', '--path', required=True, help="The base HDFS path to process.")
    parser.add_argument('-b', '--filelist-basename', default="filelist", help="The base name for the output filelists. Lists will be named basename1, basename2, ... .")
    parser.add_argument('-f', '--log-config', help="The name of a configuration file for logging.")
    parser.add_argument('-l', '--log-file', help="Name of file to have log output written to (default is stdout/stderr)")
    parser.add_argument('-v', '--log-level', default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Level of log information to output. Default is 'INFO'.")
    args = parser.parse_known_args()[0]
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=getattr(logging, args.log_level.upper()), filename=args.log_file)
    log.info("Starting processing of HDFS contents into chunked file lists")

    dirAllocations = []
    unitsSpaceAvailable = []
    processDirectoryIntoUnits(args.path, args.databox_size, dirAllocations, unitsSpaceAvailable)
    keyfunc = lambda x: x["unit"]
    for unit, dirs in itertools.groupby(sorted([dir for dir in dirAllocations if dir["unit"] != 0], key=keyfunc), keyfunc):
        with open("{0}{1}".format(args.filelist_basename, unit), "w+") as fp:
            fp.writelines([dir["path"] + '\n' for dir in dirs])

    log.info("Completed processing successfully")