from GremlinClient import GremlinClient

g = GremlinClient()

test_v1 = {"id": "3", "test": "something"}
test_v2 = {"id": "4", "test": "something"}

#g.add_vertex(test_v1)
#g.add_vertex(test_v2)

#g.add_edge("3", "4")
#g.add_edge("4", "1")
#g.add_edge("7", "3")

print(g.get_distance("7", "1"))


