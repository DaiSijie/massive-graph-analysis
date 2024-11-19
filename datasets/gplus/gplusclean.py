from os import listdir
from os.path import isfile, join
import sys

vertices = set()
remap = dict()

def parseLine(line):
	splat = line.rstrip().split(" ")
	return (splat[0], splat[1])

def computeMap():
	counter = 0
	for v in vertices:
		remap[v] = counter
		counter += 1

if __name__ == "__main__":

	if len(sys.argv) < 3:
		print("You need two arguments: input file and destination file")
		exit()

	edgeFilePath = sys.argv[1]
	destFilePath = sys.argv[2]

	edgeFile = open(edgeFilePath)

	counter = 0
	for line in edgeFile:
		counter += 1
		if line.startswith("#"):
			continue

		(u, v) = parseLine(line)
		vertices.add(u)
		vertices.add(v)

		if(counter % 100000 == 1):
			print(counter)

	edgeFile.close()
	computeMap()

	## step 2: rename
	edgeFile = open(edgeFilePath)
	destFile = open(destFilePath, "a")

	counter = 0
	for line in edgeFile:
		counter += 1
		
		if line.startswith("#"):
			continue

		(u, v) = parseLine(line)
		destFile.write(str(remap[u]) + " " + str(remap[v]) + "\n")

		if(counter % 100000 == 1):
			print(counter)

	edgeFile.close()
	destFile.close()
