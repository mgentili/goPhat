from __future__ import division
import numpy as np
import sys
#
import matplotlib.pyplot as plt
import seaborn as sns

#X = [float(x) for x in sys.stdin.readlines()]
X = sorted([np.random.normal(200, 100) for x in xrange(0, 1000)])
X = [y for y in X if y > 0]

fig = plt.figure(figsize=(12, 6))
Y = []
for i, val in enumerate(X):
    Y.append(i / len(X))
plt.plot(X, Y)
plt.xlabel('Latency')
plt.ylabel('Density')
plt.show()
