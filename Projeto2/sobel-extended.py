import cv2
import numpy as np
from pyspark import SparkContext as sc

# file_params: parameters of the rdd
def extendVertical(file_params):
    aux = [(i, (file_params[0], block)) for i,block in enumerate(np.split(file_params[1], Blocks))]
    listOfBlocks = []
    rows, cols, colors = aux[0][1][1].shape
    image = np.zeros((rows+numExt, cols, colors))
    image[:rows,:,:] = aux[0][1][1]
    image[rows:rows+numExt,:,:] = aux[1][1][1][0:numExt,:,:]
    listOfBlocks.append((aux[0][0], (file_params[0], np.uint8(image))))
    for i in range(1,len(aux)-1):
        image = np.zeros((rows+numExt*2, cols, colors))
        image[numExt:rows+numExt,:,:] = aux[i][1][1]
        image[0:numExt,:,:] = aux[i-1][1][1][rows-numExt:rows,:,:]
        image[rows+numExt:rows+numExt*2,:,:] = aux[i+1][1][1][0:numExt,:,:]
        listOfBlocks.append((aux[i][0], (file_params[0], np.uint8(image))))
    image = np.zeros((rows+numExt, cols, colors))
    image[numExt:,:,:] = aux[len(aux)-1][1][1]
    image[0:numExt,:,:] = aux[len(aux)-2][1][1][rows-numExt:rows,:,:]
    listOfBlocks.append((aux[len(aux)-1][0], (file_params[0], np.uint8(image))))
    return listOfBlocks

# file_params: parameters of the rdd
def extendHorizontal(file_params):
    aux = [((file_params[0],j),(file_params[1][0], block)) for j,block in enumerate(np.split(file_params[1][1], Blocks, axis = 1))]
    listOfBlocks = []
    rows, cols, colors = aux[0][1][1].shape
    image = np.zeros((rows, cols+numExt, colors))
    image[:,:cols,:] = aux[0][1][1]
    image[:,cols:cols+numExt,:] = aux[1][1][1][:,0:numExt,:]
    listOfBlocks.append(((file_params[0], aux[0][0][1]), (file_params[1][0], np.uint8(image))))
    for i in range(1,len(aux)-1):
        image = np.zeros((rows, cols+numExt*2, colors))
        image[:,numExt:cols+numExt,:] = aux[i][1][1]
        image[:,0:numExt,:] = aux[i-1][1][1][:,cols-numExt:cols,:]
        image[:,cols+numExt:cols+numExt*2,:] = aux[i+1][1][1][:,0:numExt,:]
        listOfBlocks.append(((file_params[0], aux[i][0][1]), (file_params[1][0], np.uint8(image))))
    image = np.zeros((rows, cols+numExt, colors))
    image[:,numExt:,:] = aux[len(aux)-1][1][1]
    image[:,0:numExt,:] = aux[len(aux)-2][1][1][:,cols-numExt:cols,:]
    listOfBlocks.append(((file_params[0], aux[len(aux)-1][0][1]), (file_params[1][0], np.uint8(image))))
    return listOfBlocks

# img: block of a image
def calculateEdges(img):
    # Calculate the derivative in the x direction
    sobelx = cv2.Sobel(img, cv2.CV_8U, 1, 0, ksize = filterSize)

    # Calculate the derivative in the y direction
    sobely = cv2.Sobel(img, cv2.CV_8U, 0, 1, ksize = filterSize)

    # Calculate the complete derivative and test it with the threshold
    return np.uint8(np.sqrt(np.square(np.float64(sobelx)) + np.square(np.float64(sobely))) < T) * 255

# R: list of blocks
def recomposeImage(R, Blocks):

    images = []
    for i in range(0,Blocks):
        blocksArray = []
        for j in range(0,Blocks):
            numBlock = i*Blocks + j
            b = R[numBlock][1]
            rows, cols = b.shape
            if i == 0:
                b = b[:rows-numExt,:]
            elif i == Blocks-1:
                b = b[numExt:,:]
            else:
                b = b[numExt:rows-numExt,:]
            if j == 0:
                b = b[:,:cols-numExt]
            elif j == Blocks-1:
                b = b[:,numExt:]
            else:
                b = b[:,numExt:cols-numExt]
            blocksArray.append(b)
        
        blockImage = np.concatenate(tuple(blocksArray), axis = 1)
        images.append(blockImage)
    
    image = np.concatenate(images, axis = 0)
    return image

# Choice of number of blocks being Blocks * Blocks
Blocks = 8

# Threshold of the edge map
T = 250

# Size of the filter and number to be extended
filterSize = 7
numExt = (filterSize - 1) / 2

# getting an instance of spark context
sc = sc()

# Obtaining rdd through of hdfs
hdfsDirectory = 'hdfs://localhost:9000/SampleImages/'
rdd = sc.binaryFiles(hdfsDirectory + '*')

# Decoding the images -- file_params (fileName, binary)
rdd = rdd.map(lambda file_params: (file_params[0], cv2.imdecode(np.asarray(bytearray(file_params[1]), dtype=np.uint8),1)))

# file_params (fileName, img) -> file_params (i, (fileName, img))
rdd = rdd.flatMap(lambda file_params: extendVertical(file_params))

# file_params (i, (fileName, img)) -> file_params ((i,j),(fileName, img))
rdd = rdd.flatMap(lambda file_params: extendHorizontal(file_params))

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