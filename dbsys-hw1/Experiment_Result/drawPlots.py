import sys
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

f = open('result.txt', 'r')

pageResult = {}
slottedPageResult = {}

for _ in xrange(40):
  line = f.readline()
  para = line.rstrip().split()

  line = f.readline()

  line = f.readline()
  temp = line.rstrip().split()
  throughput = float(temp[1])

  pageResult[(int(para[0]), float(para[1]), int(para[2]), 0)] = throughput

  line = f.readline()

  line = f.readline()
  temp = line.rstrip().split()
  space = int(temp[1])

  pageResult[(int(para[0]), float(para[1]), int(para[2]), 1)] = space

for _ in xrange(40):
  line = f.readline()
  para = line.rstrip().split()

  line = f.readline()

  line = f.readline()
  temp = line.rstrip().split()
  throughput = float(temp[1])

  slottedPageResult[(int(para[0]), float(para[1]), int(para[2]), 0)] = throughput

  line = f.readline()

  line = f.readline()
  temp = line.rstrip().split()
  space = int(temp[1])

  slottedPageResult[(int(para[0]), float(para[1]), int(para[2]), 1)] = space

pageSizes = [4096, 8 * 4096]                       # 4Kb
scaleFactors = [0.2, 0.4, 0.6, 0.8, 1.0]                     # Half of the data
workloadModes = [1, 2, 3, 4]                      # Sequential Reads

figNum = 0
for pageSize in pageSizes:
  for workloadMode in workloadModes:
    y0 = []
    for scaleFactor in scaleFactors:
      y0.append(pageResult[(pageSize, scaleFactor, workloadMode, 0)])
    
    plt.figure(figNum)
    figNum += 1
    plt.plot(scaleFactors, y0, label = 'Page Size: ' + str(pageSize) + ' Wordload Mode: ' + str(workloadMode))
    plt.ylabel('Throughput')
    plt.xlabel('Scale Factors')
    plt.legend()
    plt.savefig('page_throughput_size_' + str(pageSize) + '_mode_' + str(workloadMode) + '.jpg')

    y1 = []
    for scaleFactor in scaleFactors:
      y1.append(pageResult[(pageSize, scaleFactor, workloadMode, 1)])
    
    plt.figure(figNum)
    figNum += 1
    plt.plot(scaleFactors, y1, label = 'Page Size: ' + str(pageSize) + ' Wordload Mode: ' + str(workloadMode))
    plt.ylabel('Disk Usage')
    plt.xlabel('Scale Factors')
    plt.legend()
    plt.savefig('page_space_size_' + str(pageSize) + '_mode_' + str(workloadMode) + '.jpg')


for pageSize in pageSizes:
  for workloadMode in workloadModes:
    y0 = []
    for scaleFactor in scaleFactors:
      y0.append(slottedPageResult[(pageSize, scaleFactor, workloadMode, 0)])
    
    plt.figure(figNum)
    figNum += 1
    plt.plot(scaleFactors, y0, label = 'Page Size: ' + str(pageSize) + ' Wordload Mode: ' + str(workloadMode))
    plt.ylabel('Throughput')
    plt.xlabel('Scale Factors')
    plt.legend()
    plt.savefig('slottedpage_throughput_size_' + str(pageSize) + '_mode_' + str(workloadMode) + '.jpg')

    y1 = []
    for scaleFactor in scaleFactors:
      y1.append(slottedPageResult[(pageSize, scaleFactor, workloadMode, 1)])
    
    plt.figure(figNum)
    figNum += 1
    plt.plot(scaleFactors, y1, label = 'Page Size: ' + str(pageSize) + ' Wordload Mode: ' + str(workloadMode))
    plt.ylabel('Disk Usage')
    plt.xlabel('Scale Factors')
    plt.legend()
    plt.savefig('slottedpage_space_size_' + str(pageSize) + '_mode_' + str(workloadMode) + '.jpg')

f.close()