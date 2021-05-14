from rdflib import Graph, URIRef
from rdflib.namespace import RDFS, SOSA
from pprint import pprint

g = Graph()

g.parse("publisher.ttl", format="ttl")

qres = g.query('''
SELECT ?y ?z
WHERE {
   ?x rdf:type sosa:Observation.
   ?x sosa:hasSimpleResult ?y.
   ?x sosa:resultTime ?z.
}
ORDER BY ?z
''')

for y, z in qres:
    print(z, y, sep=" | ")
