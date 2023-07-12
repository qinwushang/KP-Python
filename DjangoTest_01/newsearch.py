import os
import json
from py2neo import Graph,Node,cypher


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'data.json')
        self.data_path1 = os.path.join(cur_dir, 'disease_cure_drug.json')
        self.data_path2 = os.path.join(cur_dir, 'drug_avoid_symptom.json')
        self.data_path3 = os.path.join(cur_dir, 'drug_lead_symptom.json')
        self.data_path4 = os.path.join(cur_dir, 'symptom_cure_drug.json')
        self.data_path5 = os.path.join(cur_dir, 'symptom_to_disease.json')
        self.g = Graph("bolt://localhost:7687")

    '''读取文件'''
    @property
    def read_nodes(self):
        # 节点
        drugs = []
        drug_food_groups = []
        drug_tastes = []
        drug_efficacys = []
        drug_ingredients = []
        drug_infos = []
        drug_person_groups =[]

        # 构建节点实体关系
        rels_drug_efficacy = []
        rels_drug_taste = []
        rels_drug_person_group = []
        rels_drug_ingredient = []
        rels_drug_food_group = []

        count = 0
        for data in open(self.data_path,"rt", encoding="utf-8"):
            print(data)
            drug_dict = {}
            count += 1
            print(count)
            data_json = json.loads(data)
            print(data_json)
            drug = data_json['drug_name']
            drug_dict['drug_name'] = drug
            drugs.append(drug)
            drug_dict['drug_ingredient'] = ''
            drug_dict['drug_person_group'] = ''
            drug_dict['drug_food_group'] = ''
            drug_dict['drug_taste'] = ''
            drug_dict['drug_efficacy'] = ''

            if 'drug_ingredient' in data_json:
                drug_ingredients += data_json['drug_ingredient']
                for drug_ingredient in data_json['drug_ingredient']:
                    rels_drug_ingredient.append([drug, drug_ingredient])

            if 'drug_person_group' in data_json:
                drug_person_groups += data_json['drug_person_group']
                for drug_person_group in data_json['drug_person_group']:
                    rels_drug_person_group.append([drug, drug_person_group])

            if 'drug_food_group' in data_json:
                drug_food_groups += data_json['drug_food_group']
                for drug_food_group in data_json['drug_food_group']:
                    rels_drug_food_group.append([drug, drug_food_group])

            if 'drug_taste' in data_json:
                drug_tastes += data_json['drug_taste']
                for drug_taste in data_json['drug_taste']:
                    rels_drug_taste.append([drug, drug_taste])

            if 'drug_efficacy' in data_json:
                drug_efficacys += data_json['drug_efficacy']
                for drug_efficacy in data_json['drug_efficacy']:
                    rels_drug_efficacy.append([drug, drug_efficacy])

            drug_infos.append(drug_dict)

        return set(drugs), set(drug_ingredients), set(drug_person_groups), set(drug_food_groups),set(drug_tastes), set(drug_efficacys), \
               drug_infos,rels_drug_ingredient,rels_drug_person_group, rels_drug_food_group, rels_drug_taste, rels_drug_efficacy

    def create_relationship_disease_cure_drug(self):
        # count = 0
        # node1 = []
        # node2 = []
        # for data in open(self.data_path1, "rt", encoding="utf-8"):
        #     count += 1
        #     print(count)
        #     data_json = json.loads(data)
        #     nodea = data_json['nodeA']
        #     print(nodea)
        #     node1.append(nodea)
        #     print(nodea)
        #     nodeb = data_json['nodeB']
        #     node2.append(nodeb)
        #     print(nodeb)
        # self.create_node("disease",node1)
        # self.create_node("drug",node2)
        count = 0
        for data in open(self.data_path1, "rt", encoding="utf-8"):
            count += 1
            print(count)
            data_json = json.loads(data)
            nodea = data_json['nodeA']
            nodeb = data_json['nodeB']
            relation = data_json['relation']
            query = "CREATE (a:drug{name:'%s'})-[r:%s{name:'治愈'}]->(b:disease{name:'%s'})" % (
            nodeb, relation, nodea)
            self.g.run(query)

    def create_relationship_drug_avoid_symptom(self):
        # count = 0
        # node1 = []
        # node2 = []
        # for data in open(self.data_path2, "rt", encoding="utf-8"):
        #     count += 1
        #     print(count)
        #     data_json = json.loads(data)
        #     nodea = data_json['nodeA']
        #     node1.append(nodea)
        #     print(nodea)
        #     nodeb = data_json['nodeB']
        #     node2.append(nodeb)
        #     print(nodeb)
        # self.create_node("drug",node1)
        # self.create_node("symptom",node2)
        count = 0
        for data in open(self.data_path2, "rt", encoding="utf-8"):
            count += 1
            print(count)
            data_json = json.loads(data)
            nodea = data_json['nodeA']
            nodeb = data_json['nodeB']
            relation = data_json['relation']
            query = "CREATE (a:drug{name:'%s'})-[r:%s{name:'避免'}]->(b:symptom{name:'%s'})" % (
            nodea, relation, nodeb)
            self.g.run(query)

    def create_relationship_drug_lead_symptom(self):
        # count = 0
        # node1 = []
        # node2 = []
        # for data in open(self.data_path3, "rt", encoding="utf-8"):
        #     count += 1
        #     print(count)
        #     data_json = json.loads(data)
        #     nodea = data_json['nodeA']
        #     node1.append(nodea)
        #     print(nodea)
        #     nodeb = data_json['nodeB']
        #     node2.append(nodeb)
        #     print(nodeb)
        # self.create_node("drug",node1)
        # self.create_node("symptom",node2)
        count = 0
        for data in open(self.data_path3, "rt", encoding="utf-8"):
            count += 1
            print(count)
            data_json = json.loads(data)
            nodea = data_json['nodeA']
            nodeb = data_json['nodeB']
            relation = data_json['relation']
            query = "CREATE (a:drug{name:'%s'})-[r:%s{name:'导致'}]->(b:symptom{name:'%s'})" % (
            nodea, relation, nodeb)
            self.g.run(query)

    def create_relationship_symptom_cure_drug(self):
        # count = 0
        # node1 = []
        # node2 = []
        # for data in open(self.data_path4, "rt", encoding="utf-8"):
        #     count += 1
        #     print(count)
        #     data_json = json.loads(data)
        #     nodea = data_json['nodeA']
        #     node1.append(nodea)
        #     print(nodea)
        #     nodeb = data_json['nodeB']
        #     node2.append(nodeb)
        #     print(nodeb)
        # self.create_node("symptom",node1)
        # self.create_node("drug",node2)
        count = 0
        for data in open(self.data_path4, "rt", encoding="utf-8"):
            count += 1
            print(count)
            data_json = json.loads(data)
            nodea = data_json['nodeA']
            nodeb = data_json['nodeB']
            relation = data_json['relation']
            query = "CREATE (a:drug{name:'%s'})-[r:%s{name:'治愈'}]->(b:symptom{name:'%s'})" % (
            nodeb, relation, nodea)
            self.g.run(query)

    def create_relationship_symptom_to_disease(self):
        # count = 0
        # node1 = []
        # node2 = []
        # for data in open(self.data_path5, "rt", encoding="utf-8"):
        #     count += 1
        #     print(count)
        #     data_json = json.loads(data)
        #     nodea = data_json['nodeA']
        #     node1.append(nodea)
        #     print(nodea)
        #     nodeb = data_json['nodeB']
        #     node2.append(nodeb)
        #     print(nodeb)
        # self.create_node("symptom",node1)
        # self.create_node("disease",node2)
        count = 0
        for data in open(self.data_path5, "rt", encoding="utf-8"):
            count += 1
            print(count)
            data_json = json.loads(data)
            nodea = data_json['nodeA']
            nodeb = data_json['nodeB']
            relation = data_json['relation']
            query = "CREATE (a:disease{name:'%s'})-[r:%s{name:'导致'}]->(b:symptom{name:'%s'})" % (
            nodeb, relation,nodea)
            self.g.run(query)

    '''建立节点'''
    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    '''创建实体关联边'''
    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    def query_all_relationship(self, num):
        k = (int(num))
        query = "MATCH(a)-[r]->(b) RETURN  id(a),labels(a),properties(a),id(r),type(r),properties(r)" \
                 ",id(b), labels(b),properties(b)  limit %s" % k
        result1s = self.g.run(query)
        a = []
        for result1 in result1s:
            dict1 = {}
            dict1["a"] = {}
            dict1["r"] = {}
            dict1["b"] = {}
            dict1["a"]["labels"] = []
            dict1["a"]["properties"] = {}
            dict1["a"]["properties"]["name"] = dict(result1)["properties(a)"]["name"]
            dict1["a"]["labels"] = dict(result1)["labels(a)"]
            dict1["a"]["identity"] = dict(result1)["id(a)"]
            dict1["r"]["properties"] = dict(result1)["properties(r)"]
            dict1["r"]["type"] = dict(result1)["type(r)"]
            dict1["r"]["identity"] = dict(result1)["id(r)"]
            dict1["r"]["start"] = dict(result1)["id(a)"]
            dict1["r"]["end"] = dict(result1)["id(b)"]
            dict1["b"]["identity"] = dict(result1)["id(b)"]
            dict1["b"]["properties"] = dict(result1)["properties(b)"]
            dict1["b"]["labels"] = dict(result1)["labels(b)"]
            a.append(dict1)
        # f = open("data100.json", "w", encoding="utf-8")
        # json.dump(a, f, ensure_ascii=False)
        return a

    def query_onestart_relationship(self,start_node_name,num):
        query = "MATCH (a{name:'" + start_node_name + "'})-[r]->(b) RETURN id(a),labels(a),properties(a),id(r)," \
                                                      "type(r),properties(r),id(b), labels(b),properties(b) limit %s"\
                % (int(num))
        result1s = self.g.run(query)
        a = []
        for result1 in result1s:
            dict1 = {}
            dict1["a"] = {}
            dict1["r"] = {}
            dict1["b"] = {}
            dict1["a"]["labels"] = []
            dict1["a"]["properties"] = {}
            dict1["a"]["properties"]["name"] = dict(result1)["properties(a)"]["name"]
            dict1["a"]["labels"] = dict(result1)["labels(a)"]
            dict1["a"]["identity"] = dict(result1)["id(a)"]
            dict1["r"]["properties"] = dict(result1)["properties(r)"]
            dict1["r"]["type"] = dict(result1)["type(r)"]
            dict1["r"]["identity"] = dict(result1)["id(r)"]
            dict1["r"]["start"] = dict(result1)["id(a)"]
            dict1["r"]["end"] = dict(result1)["id(b)"]
            dict1["b"]["identity"] = dict(result1)["id(b)"]
            dict1["b"]["properties"] = dict(result1)["properties(b)"]
            dict1["b"]["labels"] = dict(result1)["labels(b)"]
            a.append(dict1)
        # f = open("demo1.json", "w", encoding="utf-8")
        # json.dump(a, f, ensure_ascii=False)
        return a

    def query_oneend_relationship(self, end_node_name,num):
        query = "MATCH (a)-[r]->(b{name:'" + end_node_name + "'}) RETURN id(a),labels(a),properties(a),id(r),type(r),properties(r)" \
                                                      ",id(b), labels(b),properties(b) limit %s" % (int(num))
        result1s = self.g.run(query)
        a = []
        for result1 in result1s:
            dict1 = {}
            dict1["a"] = {}
            dict1["r"] = {}
            dict1["b"] = {}
            dict1["a"]["labels"] = []
            dict1["a"]["properties"] = {}
            dict1["a"]["properties"]["name"] = dict(result1)["properties(a)"]["name"]
            dict1["a"]["labels"] = dict(result1)["labels(a)"]
            dict1["a"]["identity"] = dict(result1)["id(a)"]
            dict1["r"]["properties"] = dict(result1)["properties(r)"]
            dict1["r"]["type"] = dict(result1)["type(r)"]
            dict1["r"]["identity"] = dict(result1)["id(r)"]
            dict1["r"]["start"] = dict(result1)["id(a)"]
            dict1["r"]["end"] = dict(result1)["id(b)"]
            dict1["b"]["identity"] = dict(result1)["id(b)"]
            dict1["b"]["properties"] = dict(result1)["properties(b)"]
            dict1["b"]["labels"] = dict(result1)["labels(b)"]
            a.append(dict1)
        # f = open("demo1.json", "w",encoding="utf-8")
        # json.dump(a, f, ensure_ascii=False)
        return a

    def query_one_relation(self,node_name,num):
        query = "MATCH (a{name:'" + node_name + "'})-[r]->(b) RETURN id(a),labels(a),properties(a),id(r),type(r),properties(r)" \
                 ",id(b), labels(b),properties(b) limit %s" % (int(num))
        result1s = self.g.run(query)
        a = []
        for result1 in result1s:
            dict1 = {}
            dict1["a"] = {}
            dict1["r"] = {}
            dict1["b"] = {}
            dict1["a"]["labels"] = []
            dict1["a"]["properties"] = {}
            dict1["a"]["properties"]["name"] = dict(result1)["properties(a)"]["name"]
            dict1["a"]["labels"] = dict(result1)["labels(a)"]
            dict1["a"]["identity"] = dict(result1)["id(a)"]
            dict1["r"]["properties"] = dict(result1)["properties(r)"]
            dict1["r"]["type"] = dict(result1)["type(r)"]
            dict1["r"]["identity"] = dict(result1)["id(r)"]
            dict1["r"]["start"] = dict(result1)["id(a)"]
            dict1["r"]["end"] = dict(result1)["id(b)"]
            dict1["b"]["identity"] = dict(result1)["id(b)"]
            dict1["b"]["properties"] = dict(result1)["properties(b)"]
            dict1["b"]["labels"] = dict(result1)["labels(b)"]
            a.append(dict1)
        # f = open("demo1.json", "w",encoding="utf-8")
        # json.dump(a, f, ensure_ascii=False)
        return a

    '''创建知识图谱中心药品的节点'''
    def create_drugs_nodes(self, drug_infos):
        count = 0
        for drug_dict in drug_infos:
            # node = Node("drug", drug_name=drug_dict['drug_name'], drug_ingredient=drug_dict['drug_ingredient'],
            #             drug_person_group=drug_dict['drug_person_group'], drug_food_group=drug_dict['drug_food_group'],
            #             drug_taste=drug_dict['drug_taste'],drug_efficacy=drug_dict['drug_efficacy'])
            node = Node("drug", drug_name=drug_dict['drug_name'])
            self.g.create(node)
            count += 1
            print(count)
        return

    '''创建知识图谱实体节点类型schema'''
    def create_graphnodes(self):
        drugs, drug_ingredients, drug_person_groups, drug_food_groups, drug_tastes, drug_efficacys,drug_infos, \
        rels_drug_ingredient, rels_drug_person_group, rels_drug_food_group, rels_drug_taste, rels_drug_efficacy= self.read_nodes
        # self.create_drugs_nodes(drug_infos)
        self.create_node('drug', drugs)
        print(len(drugs))
        # self.create_node('drug_ingredient', drug_ingredients)
        # print(len(drug_ingredients))
        # self.create_node('drug_person_group', drug_person_groups)
        # print(len(drug_person_groups))
        # self.create_node('drug_food_group', drug_food_groups)
        # print(len(drug_food_groups))
        # self.create_node('drug_taste', drug_tastes)
        # print(len(drug_tastes))
        # self.create_node('drug_efficacy', drug_efficacys)
        # print(len(drug_efficacys))
        return


    '''创建实体关系边'''
    def create_graphrels(self):
        drugs, drug_ingredients, drug_person_groups, drug_food_groups, drug_tastes, drug_efficacys, drug_infos, \
        rels_drug_ingredient, rels_drug_person_group, rels_drug_food_group, rels_drug_taste, rels_drug_efficacy = self.read_nodes
        self.create_relationship('drug', 'drug_ingredient', rels_drug_ingredient, 'ingredient', '成分')
        self.create_relationship('drug', 'drug_person_group', rels_drug_person_group, 'person_group', '适合人群')
        self.create_relationship('drug', 'drug_food_group', rels_drug_food_group, 'food_group', '忌吃')
        self.create_relationship('drug', 'drug_taste', rels_drug_taste, 'drug_taste', '口感')
        self.create_relationship('drug', 'drug_efficacy', rels_drug_efficacy, 'drug_efficacy', '疗效')


if __name__ == '__main__':
    handler = MedicalGraph()
    # handler.create_graphnodes()
    # handler.create_graphrels()
    # handler.create_relationship_disease_cure_drug()
    # handler.create_relationship_drug_avoid_symptom()
    handler.create_relationship_drug_lead_symptom()
    # handler.create_relationship_symptom_cure_drug()
    # handler.create_relationship_symptom_to_disease()
    print("over!")



