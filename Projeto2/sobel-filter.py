import cv2
import numpy as np
from pyspark import SparkContext as sc

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

# img: block of a image
def calculateEdges(img):
    # Calculate the derivative in the x direction
    sobelx = cv2.Sobel(img, cv2.CV_8U, 1, 0, ksize = filterSize)

    # Calculate the derivative in the y direction
    sobely = cv2.Sobel(img, cv2.CV_8U, 0, 1, ksize = filterSize)

    # Calculate the complete derivative and test it with the threshold
    return np.uint8(np.sqrt(np.square(np.float64(sobelx)) + np.square(np.float64(sobely))) < T) * 255

# Choice of number of blocks being Blocks * Blocks
Blocks = 8

# Threshold of the edge map
T = 250

# Size of the filter
filterSize = 7

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

# Transforming the images to a gray color scale -- rdd input: file_params ((i,j),(fileName, img))
rdd = rdd.map(lambda file_params: ((file_params[0][0],file_params[0][1]),(file_params[1][0], cv2.cvtColor(file_params[1][1], cv2.COLOR_BGR2GRAY))))

# Applying the sobel filter in each block -- rdd input: file_params ((i,j),(fileName, img)). Returns (fileName, ((i,j), img))
rdd = rdd.map(lambda file_params: (file_params[1][0], ((file_params[0][0], file_params[0][1]), calculateEdges(file_params[1][1]))))

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