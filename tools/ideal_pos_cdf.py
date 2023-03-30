import json
import matplotlib.pyplot as plt
import numpy as np
import sys

# Get the filename from the command line argument
filename = sys.argv[1]

# Read in the JSON file
with open(filename, 'r') as f:
    data = json.load(f)

# Compute the cumulative distribution function
values = sorted(data.values())
cdf = np.cumsum(values) / np.sum(values)

# Plot the CDF
plt.step(sorted(data.keys()), cdf, where='post')
plt.xlabel('Values')
plt.ylabel('CDF')
plt.savefig("./results/ideal_pos_diff.png")
