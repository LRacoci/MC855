import cv2
import numpy as np
from pyspark import SparkContext as sc

sc = sc()

rdd = sc.binaryFiles('hdfs://localhost:9000/SampleImages/*')

rdd = rdd.map(lambda (fileName, binary): (fileName, cv2.imdecode(np.asarray(bytearray(binary), dtype=np.uint8),1)))

rdd = rdd.map(lambda (fileName, image): (fileName, cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)))

rdd = rdd.map(lambda (fileName, image): (fileName, cv2.Sobel(image, cv2.CV_8U, 1, 0, ksize = 3), cv2.Sobel(image, cv2.CV_8U, 0, 1, ksize = 3)))

# build rdd and take one element for testing purpose
for R in rdd.collect():
	cv2.imshow('R',R[1])
	cv2.waitKey(0)
	cv2.imshow('R',R[2])
	cv2.waitKey(0)
'''
'''
print("Finished")