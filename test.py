from GremlinClient import GremlinClient

g = GremlinClient()

g.delete_graph()

test_v1 = {"id": "21", "test": "something"}
test_v2 = {"id": "30", "test": "something"}
test_v3 = {"id": "1", "test2": "this"}

#g.add_vertex(test_v1)
#g.add_vertex(test_v2)

#g.add_edge("3", "4")
#g.add_edge("4", "1")
g.add_edge(test_v1, test_v2)

#print(g.get_distance("7", "1"))

#print(g.check_vertex_exists("7"))

#g.update_vertex(test_v3)

#print(g.get_vertex(test_v2))



