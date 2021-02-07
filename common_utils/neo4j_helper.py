from py2neo import Graph,Node,Subgraph
import datetime

class Neo4jHelper(object):
    
    def __init__(self, graph):
        self.graph = graph 
        self.node_matcher = NodeMatcher(graph)
        
    def upsert_node(self, label, properties):
        
        exist_node = self.node_matcher.match(label).where("_._key='{}'".format(properties["_key"])).first()
        
        if not exist_node:
            node = Node(label)
            for key in properties:
                node[key] = properties[key]
            self.graph.create(node)
        else:
            for key in properties:
                exist_node[key] = properties[key]
            sub_graph = Subgraph([exist_node])
            self.graph.push(sub_graph)
            
            

                
                
                
            