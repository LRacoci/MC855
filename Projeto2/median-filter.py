import cv2
import numpy as np
from pyspark import SparkContext as sc

sc = sc()

rdd = sc.binaryFiles('hdfs://localhost:9000/ibagens/*.jpg')

rdd = rdd.map(lambda (fileName, binary): (fileName, cv2.imdecode(np.asarray(bytearray(binary), dtype=np.uint8),1)))

rdd = rdd.flatMap(lambda (fileName, img): [(i, (fileName, block)) for i,block in enumerate(np.split(img, 2))])
rdd = rdd.flatMap(lambda (i,(fileName, img)): [((i,j),(fileName, block)) for j,block in enumerate(np.split(img, 2, axis = 1))])

#rdd = rdd.map(lambda ((i,j),(fileName, img)): ((i,j),(fileName, cv2.medianBlur(img, 7))))
rdd = rdd.map(lambda ((i,j),(fileName, img)): ((i,j),(fileName, cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))))

rdd = rdd.map(lambda ((i,j),(fileName, img)): ((i,j),(fileName, cv2.Sobel(img, cv2.CV_8U, 1, 0, ksize = 3))))

print rdd.take(1)

# build rdd and take one element for testing purpose
for R in rdd.collect():
	cv2.imshow('R',R[1][1])
	cv2.waitKey(0)
'''
'''
print("Finished")