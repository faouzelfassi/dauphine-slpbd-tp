graph = [(1,4), (2,1), (2,5), (3,2), (4,2), (5,3)]
g = spark.sparkContext.parallelize(graph)
# g.collect()
gInverse = g.map(lambda x: (x[1],x[0]))
# Array[(Int, Int)] = Array((4,1), (1,2), (5,2), (2,3), (2,4), (3,5))
opentriangles = g.join(gInverse)
# res2: Array[(Int, (Int, Int))] = Array((4,(2,1)), (1,(4,2)), (5,(3,2)), (2,(1,3)), (2,(1,4)), (2,(5,3)), (2,(5,4)), (3,(2,5)))

trianglesToClose = opentriangles.map(lambda x: (x[1] , x))
# trianglesToClose.collect()
# res4: Array[((Int, Int), (Int, (Int, Int)))] = Array(((2,1),(4,(2,1))), ((4,2),(1,(4,2))), ((3,2),(5,(3,2))), ((1,3),(2,(1,3))), ((1,4),(2,(1,4))), ((5,3),(2,(5,3))), ((5,4),(2,(5,4))), ((2,5),(3,(2,5))))

triangles = trianglesToClose.join(g.map(lambda x: (x, 1))).map(lambda x: (x[1][0], x[1][1]))

triangles.count()
