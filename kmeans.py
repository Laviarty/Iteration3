#from sqlalchemy import create_engine
import pandas as pd

import time

import numpy as np
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import pairwise_distances_argmin


##############################################################################
# Generate sample data

reader = pd.read_csv('kmeans.csv', sep=';',header=0)
rawdata2 = pd.DataFrame(reader,columns=["date","text"])

rawdata = pd.DataFrame(columns=['text','date'])

rawdata['text'] = rawdata2['text']
rawdata['date'] = rawdata2['date']


X = np.array(rawdata) 


##############################################################################
# Compute clustering with Means
n_clusters=8
k_means = KMeans(init='k-means++', n_clusters=8, n_init=10)
t0 = time.time()
k_means.fit(X)
t_batch = time.time() - t0
                   
labels = k_means.labels_
#print(labels)

predict = k_means.predict(X)
#print(predict)

centers = k_means.cluster_centers_
#print(centers)                  

##############################################################################
# Plot result

fig = plt.figure(figsize=(24, 8))
fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)
colors = ['#4EACC5', '#FF9C34', '#4E9A06','#9a1906','#ecfc11','#2cfc10','#6b2102','#00fffa']

# We want to have the same colors for the same cluster from the
# MiniBatchk_means and the k_means algorithm. Let's pair the cluster centers per
# closest one.
k_means_cluster_centers = np.sort(k_means.cluster_centers_, axis=0)
#mbk_means_cluster_centers = np.sort(mbk.cluster_centers_, axis=0)
k_means_labels = pairwise_distances_argmin(X, k_means_cluster_centers)
#mbk_means_labels = pairwise_distances_argmin(X, mbk_means_cluster_centers)
#order = pairwise_distances_argmin(k_means_cluster_centers,mbk_means_cluster_centers)

# k_means
ax = fig.add_subplot(1, 3, 1)
for k, col in zip(range(n_clusters), colors):
    my_members = k_means_labels == k
    cluster_center = k_means_cluster_centers[k]
    ax.plot(X[my_members, 0], X[my_members, 1], 'w',
            markerfacecolor=col, marker='.')
    ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
            markeredgecolor='k', markersize=6)
ax.set_title('k_means')
ax.set_xticks(())
ax.set_yticks(())
#plt.text(-3.5, 1.8,  'train time: %.2fs\ninertia: %f' % (
 #   t_batch, k_means.inertia_))

plt.show()
plt.savefig('datecount.png',bbox_inches='tight')