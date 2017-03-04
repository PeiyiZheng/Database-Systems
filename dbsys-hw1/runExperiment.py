#from Database                import Database
import os
import sys
from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage

# Path to the folder containing csvs (on ugrad cluster)
dataDir = '/home/cs416/datasets/tpch-sf0.01/'
fileDir = './data/'

# Pick a page class, page size, scale factor, and workload mode:
#StorageFile.defaultPageClass = Page   # Contiguous Page

def runExperiment(dataDir, fileDir, pageClass):
    StorageFile.defaultPageClass = pageClass
    pageSizes = [4096, 8 * 4096]                       # 4Kb
    scaleFactors = [0.2, 0.4, 0.6, 0.8, 1.0]                     # Half of the data
    workloadModes = [1, 2, 3, 4]                      # Sequential Reads

    # Run! Throughput will be printed afterwards.
    # Note that the reported throughput ignores the time
    # spent loading the dataset.
    for pageSize in pageSizes:
        for scaleFactor in scaleFactors:
            for workloadMode in workloadModes:
                print(pageSize, scaleFactor, workloadMode)
                wg = WorkloadGenerator()
                wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)

                space = 0
                for filename in os.listdir(fileDir):
                    if filename.endswith(".rel"):
                        # B to KB
                        fileSize = os.path.getsize(os.path.join(fileDir, filename)) >> 10
                        space += fileSize

                print("Space: {0}".format(space))

runExperiment(dataDir, fileDir, Page)

runExperiment(dataDir, fileDir, SlottedPage)
