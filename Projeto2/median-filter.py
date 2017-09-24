import cv2
import numpy as np
from pyspark import SparkContext as sc

# R: list of blocks
def recomposeImage(R, numOfBlocks):

    images = []
    for i in range(0,numOfBlocks):
        blocksArray = []
        for j in range(0,numOfBlocks):
            numBlock = i*numOfBlocks + j
            blocksArray.append(R[numBlock][1])
        
        blockImage = np.concatenate(tuple(blocksArray), axis = 1)
        images.append(blockImage)
    
    image = np.concatenate(images, axis = 0)
    return image

# Choice of number of blocks
numOfBlocks = 8

# getting an instance of spark context
sc = sc()

# Obtaining rdd through of hdfs
hdfsDirectory = 'hdfs://localhost:9000/user/RodrigoMaximo/Imagens/'
rdd = sc.binaryFiles(hdfsDirectory + '*.jpg')

# Obtaining rdd using a local directory
# rdd = sc.binaryFiles('imagens/*.jpg')

# Decoding the images -- file_params (fileName, binary)
rdd = rdd.map(lambda file_params: (file_params[0], cv2.imdecode(np.asarray(bytearray(file_params[1]), dtype=np.uint8),1)))

# file_params (fileName, img) -> file_params (i, (fileName, img))
rdd = rdd.flatMap(lambda file_params: [(i, (file_params[0], block)) for i,block in enumerate(np.split(file_params[1], numOfBlocks))])

# file_params (i, (fileName, img)) -> file_params ((i,j),(fileName, img))
rdd = rdd.flatMap(lambda file_params: [((file_params[0],j),(file_params[1][0], block)) for j,block in enumerate(np.split(file_params[1][1], numOfBlocks, axis = 1))])

#rdd = rdd.map(lambda ((i,j),(fileName, img)): ((i,j),(fileName, cv2.medianBlur(img, 7))))

# Transforming the images to a gray color scale -- rdd input: file_params ((i,j),(fileName, img))
rdd = rdd.map(lambda file_params: ((file_params[0][0],file_params[0][1]),(file_params[1][0], cv2.cvtColor(file_params[1][1], cv2.COLOR_BGR2GRAY))))

# Applying the sobel filter in each block -- rdd input: file_params ((i,j),(fileName, img)). Retuns (fileName, ((i,j), img))
rdd = rdd.map(lambda file_params: (file_params[1][0], ((file_params[0][0], file_params[0][1]), cv2.Sobel(file_params[1][1], cv2.CV_8U, 1, 0, ksize = 3))))

rdd = rdd.groupByKey()

# print(rdd.take(4))

images = []

# rdd.collect returns an array of a tuples in format: (imageName, [blocks]), being a block in format: ((i,j), contentImg)
for R in rdd.collect():
    imgName = R[0]
    listOfBlocks = list(R[1])
    images.append((imgName,recomposeImage(listOfBlocks, numOfBlocks)))

# build rdd and take one element for testing purpose
for image in images:
	# print(image)
	cv2.imshow(image[0][40:],image[1])
	cv2.waitKey(0)

'''
'''
print("Finished")