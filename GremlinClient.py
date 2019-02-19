from gremlin_python.driver import client, serializer
import sys, traceback
import json

class GremlinClient:

    def __init__(self):
        self.client = ""
        self.get_client()

    def get_rec(self, preferences):

        """
        get recommendations based on inputted preferences
        :param preferences: list of strings indicating user preferences
        :return:
        """

    def get_client(self):

        """
        Initializes the client for db interaction
        :return:
        """

        try:
            c = client.Client(
                'wss://topicgraph.gremlin.cosmosdb.azure.com:443/','g',
                username="/dbs/topicgraph/colls/topicgraph1",
                password="5RtUEjyTIkBZ4Hs0HXCX8yB9ify1angL11S4w7By22sXhzORFKWgcnzny62GnUS6MFWox9qDWUTAUATDqS6UyA==",
                message_serializer=serializer.GraphSONSerializersV2d0()
                               )

            self.client = c


        except:

            print("oops")

    def add_vertex(self, params):
        """
        add a vertex using specific parameters
        :param params: dict with named parameters
        :return:
        """

        keys = list(params.keys())

        id = keys[0]
        base = "g.addV().property(id, '%s')" % params[id]

        for i in keys[1:]:
            new = ".property('%s', '%s')" % (i, params[i])
            base = base + new

        print(base)

        try:

            callback = self.client.submitAsync(base)

            if callback.result() is not None:

                print(callback.result().one())
                print("Success at adding vertex")

            else:

                print("Something went wrong when adding vertex")

        except Exception as e:

            print("Error adding vertex", params[id])
            print(e)


    def add_edge(self, id1, id2):

        """
        add edge between two specified vertices
        :param id1:
        :param id2:
        :return:
        """
        base1 = "g.V('%s').addE('related_to').to(g.V('%s'))" % (id1, id2)
        base2 = "g.V('%s').addE('related_to').to(g.V('%s'))" % (id2, id1)

        edges = [base1, base2]

        for i in edges:

            try:

                callback = self.client.submitAsync(i)

                if callback.result() is not None:

                    print(callback.result().one())
                    print("Success at adding edge")

                else:

                    print("Something went wrong when adding edge")

            except Exception as e:

                print("Something went wrong when adding edge between", id1, "and", id2)
                print(e)

    def get_distance(self, id1, id2):

        base = "g.V('%s').repeat(out().simplePath()).until(has('id', '%s')).path().limit(1)" % (id1, id2)

        try:

            distance = self.client.submitAsync(base)
            if distance.result() is not None:
                distance = distance.result().one()
                
                return (len(distance[0]["labels"]) - 2)

        except Exception as e:

            print("Something went wrong when trying to get the distance between", id1, "and", id2)
            print(e)




