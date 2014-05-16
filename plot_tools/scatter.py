from __future__ import division
import sys
#
import matplotlib.pyplot as plt
import seaborn as sns

args = sys.argv[1:]
labels = args[0].split(', ')
files = args[1:]
data = [[map(float, x.split(', ')) for x in open(fn).readlines()] for fn in files]

fig, axes = plt.subplots(1, 1, figsize=(10, 6))
color_cycle = axes._get_lines.color_cycle
###
biggestX = 0
biggestY = 0
for label, points in zip(labels, data):
    start = [x for _, x, y in points]
    latency = [y for _, x, y in points]
    biggestX = max(biggestX, max(start))
    biggestY = max(biggestY, max(latency))
    col = next(color_cycle)
    plt.scatter(start, latency, label=label, color=col, alpha=0.5)
    plt.axvline(start[-1], color=col, alpha=0.5)
###
plt.xlabel('Time')
plt.ylabel('Latency')
plt.xlim((0, biggestX))
plt.ylim((0, biggestY))
plt.legend()
plt.show()
