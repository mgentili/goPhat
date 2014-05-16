from __future__ import division
import numpy as np
import sys
#
import matplotlib.pyplot as plt
import seaborn as sns

args = sys.argv[1:]
labels = args[0].split(', ')
files = args[1:]
data = [[map(float, x.split(', ')) for x in open(fn).readlines()] for fn in files]

fig, axes = plt.subplots(1, 1, sharex=True, sharey=True, figsize=(10, 6))
color_cycle = axes._get_lines.color_cycle
biggestX = 0
###
for label, points in zip(labels, data):
    start = [x for _, x, y in points]
    latency = sorted([y for _, x, y in points])
    print label, latency[-50:]
    X = latency
    biggestX = max(biggestX, max(latency))
    Y = []
    for i, val in enumerate(X):
        Y.append(i / len(X))
    plt.plot(X, Y, label=label, color=next(color_cycle), alpha=0.5)
###
plt.xlabel('Latency')
plt.ylabel('Density')
plt.xlim((0, biggestX))
plt.ylim((0, 1))
plt.legend()
plt.show()
