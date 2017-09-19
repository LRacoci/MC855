import cv2
import numpy as np
from pyspark import SparkContext as sc

sc = sc()

rdd = sc.binaryFiles('hdfs://localhost:9000/ibagens/*.jpg')

rdd = rdd.map(lambda (fileName, binary): (fileName, cv2.imdecode(np.asarray(bytearray(binary), dtype=np.uint8),1)))

# build rdd and take one element for testing purpose
for R in rdd.collect():
	cv2.imshow('R',R[1])
	cv2.waitKey(0)
'''
'''
print("Finished")