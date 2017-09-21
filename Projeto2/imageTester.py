# Codigo para leitura da imagem pgm

import numpy as np
from pyspark import SparkContext as sc

sc = sc()

# Transformando o arquivo em um RDD (leitura como arquivo texto)
images = sc.textFile(name = 'image1.pgm')

# Agrupar num array de array e depois mapear pra realizar o sobel.
for x in images.take(5):
    print(x)

