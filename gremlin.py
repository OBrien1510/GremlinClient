import GremlinClient
import hug

@hug.get('/course_rec')
def main(preferences):

    r = GremlinClient()