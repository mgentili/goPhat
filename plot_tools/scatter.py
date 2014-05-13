from __future__ import division
import numpy as np
import sys
#
import matplotlib.pyplot as plt
import seaborn as sns

#data = [map(float, x.split()) for x in sys.stdin.readlines()]
data = [(x, 2 * x + 40 + 10 * np.random.random()) for x in xrange(0, 100)]
fig = plt.figure(figsize=(12, 6))
X = [x for x, y in data]
Y = [y for x, y in data]
plt.scatter(X, Y)
plt.xlabel('Time')
plt.ylabel('Latency')
plt.show()
