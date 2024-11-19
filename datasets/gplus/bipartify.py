import sys

def acceptEdge(v1, v2):
	return (v1 % 2) != (v2 % 2)

if __name__ == "__main__":

	# catch arguments
	if len(sys.argv) < 3:
		print("two arguments needed: origin, dest")
		exit()

	origin = sys.argv[1]
	dest = sys.argv[2]

	# copy files
	originfile = open(origin)
	destfile = open(dest, "a")

	counter = 0

	firstLine = True
	for line in originfile:
		if(line[0] == "#"): # indicates a comment
			continue

		splat = line.split(" ")
		v1 = int(splat[0])
		v2 = int(splat[1])
		if(acceptEdge(v1, v2)):
			if(firstLine):
				firstLine = False
			else:
				destfile.write("\n")

			if(v1 % 2 == 1):
				destfile.write(str(v1) + " " + str(v2))
			else:
				destfile.write(str(v2) + " " + str(v1))

		counter += 1
		if(counter % 100000 == 1):
			print(counter)

	originfile.close()
	destfile.close()