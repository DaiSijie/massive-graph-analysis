from os import listdir
from os.path import isfile, join
import sys

leftVertices = set()
rightVertices = set()

leftMap = dict()
rightMap = dict()

def parseLine(line):
	splat = line.rstrip().split("	")

	if(len(splat) != 2):
		splat = line.rstrip().split(" ")

	return (splat[0], splat[1])

def computeMaps():
	counter = 0
	for left in leftVertices:
		leftMap[left] = counter
		counter += 1
	for right in rightVertices:
		rightMap[right] = counter
		counter += 1

def convertLine(line):
	splat = line.rstrip().split("	")

	if(len(splat) != 2):
		splat = line.rstrip().split(" ")

	return str(leftMap[splat[0]]) + " " + str(rightMap[splat[1]])

if __name__ == "__main__":

	if len(sys.argv) < 3:
		print("You need two arguments: input file and destination file")
		exit()

	edgeFilePath = sys.argv[1]
	destFilePath = sys.argv[2]

	print(edgeFilePath, destFilePath)

	## step 1: prepare the maps an check for bipartiteness
	edgeFile = open(edgeFilePath)

	counter = 0
	for line in edgeFile:
		counter += 1
		if line.startswith("#"):
			continue

		(u, v) = parseLine(line)
		leftVertices.add(u)
		rightVertices.add(v)
		if v in leftVertices or u in rightVertices:
			print("actually it's not bipartite")

		if(counter % 100000 == 1):
			print(counter)

	edgeFile.close()
	computeMaps()

	## step 2: rename
	edgeFile = open(edgeFilePath)
	destFile = open(destFilePath, "a")

	counter = 0
	for line in edgeFile:
		counter += 1
		
		if line.startswith("#"):
			continue
		newLine = convertLine(line.rstrip())
		destFile.write(newLine + "\n")

		if(counter % 100000 == 1):
			print(counter)

	edgeFile.close()
	destFile.close()

	print("Done. Left size = ", len(leftVertices), " right size ", len(rightVertices))
