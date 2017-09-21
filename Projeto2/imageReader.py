import cv2
import numpy as np
from pyspark import SparkContext as sc

sc = sc()

# build rdd and take one element for testing purpose
rdd = sc.binaryFiles('hdfs://localhost:9000/SampleImages/*').collect()

for L in rdd:
  # convert to bytearray and then to np array
  file_bytes = np.asarray(bytearray(L[1]), dtype=np.uint8)

  # use opencv to decode the np bytes array 
  R = cv2.imdecode(file_bytes,1)

  cv2.imshow('teste', R)
  cv2.waitKey()