import cv2
import numpy as np
from pyspark import SparkContext as sc

# img: block of a image
def calculateMedian(img):
    return cv2.medianBlur(img, filterSize)

# R: list of blocks
def recomposeImage(R, Blocks):

    images = []
    for i in range(0,Blocks):
        blocksArray = []
        for j in range(0,Blocks):
            numBlock = i*Blocks + j
            blocksArray.append(R[numBlock][1])
        
        blockImage = np.concatenate(tuple(blocksArray), axis = 1)
        images.append(blockImage)
    
    image = np.concatenate(images, axis = 0)
    return image

# Choice of number of blocks being Blocks * Blocks
Blocks = 8

# Size of the filter
filterSize = 13

# getting an instance of spark context
sc = sc()

# Obtaining rdd through of hdfs
hdfsDirectory = 'hdfs://localhost:9000/SampleImages/'
rdd = sc.binaryFiles(hdfsDirectory + '*')

# Decoding the images -- file_params (fileName, binary)
rdd = rdd.map(lambda file_params: (file_params[0], cv2.imdecode(np.asarray(bytearray(file_params[1]), dtype=np.uint8),1)))

# file_params (fileName, img) -> file_params (i, (fileName, img))
rdd = rdd.flatMap(lambda file_params: [(i, (file_params[0], block)) for i,block in enumerate(np.split(file_params[1], Blocks))])

# file_params (i, (fileName, img)) -> file_params ((i,j),(fileName, img))
rdd = rdd.flatMap(lambda file_params: [((file_params[0],j),(file_params[1][0], block)) for j,block in enumerate(np.split(file_params[1][1], Blocks, axis = 1))])

# Applying the sobel filter in each block -- rdd input: file_params ((i,j),(fileName, img)). Returns (fileName, ((i,j), img))
rdd = rdd.map(lambda file_params: (file_params[1][0], ((file_params[0][0], file_params[0][1]), calculateMedian(file_params[1][1]))))

# Group the blocks of the same images
rdd = rdd.groupByKey()

# Recreate de blocks into images -- rdd input: file_params (fileName, listOfBlocks)
rdd = rdd.map(lambda file_params: (file_params[0], recomposeImage(list(file_params[1]), Blocks)))

# build rdd and take one element for testing purpose
for R in rdd.collect():
	cv2.imshow(R[0],R[1])
	cv2.waitKey(0)

'''
'''
print("Finished")