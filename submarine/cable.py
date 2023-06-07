import os
import json
import queue
from geopy.distance import geodesic


class SubmarineCableNode:
    def __init__(self, geo):
        self.geo = geo
        self.lp = False
        self.lp_id = None
        self.lp_country = None
        self.neighbors = []
    
    def _same_node(self, node):
        try:
            if (abs(node.geo[0] - self.geo[0]) <= 0.05 or abs(360 - abs(node.geo[0] - self.geo[0])) <= 0.05) and (abs(node.geo[1] - self.geo[1]) <= 0.05 or abs(360 - abs(node.geo[1] - self.geo[1])) <= 0.05):
                return True
        except:
            return False
    
    def existed(self, nodes):
        for node in nodes:
            if self._same_node(node):
                return True, node
        return False, None
    
    def is_lp(self, lps):
        for lp_id in lps.keys():
            new_node = SubmarineCableNode(lps[lp_id]["geo"])
            if self._same_node(new_node):
                return True, lp_id
        return False, ''
    
    def format_coord(self):
        return (
            int(self.geo[0] * 100) / 100,
            int(self.geo[1] * 100) / 100,
        )


class SubmarineCable:
    def __init__(self, cable_id):
        self.BASIC_DIR = os.path.expanduser('~/.submarine/')
        self.rfs = None
        self.geo = None
        self.cable_id = cable_id
        self.LP_DIR = os.path.join(self.BASIC_DIR, 'landing-point')
        self.CABLE_DIR = os.path.join(self.BASIC_DIR, 'cable')
        self.LP_GEO_PATH = os.path.join(self.LP_DIR, 'landing-point-geo.json')
        self.CABLE_GEO_PATH = os.path.join(self.CABLE_DIR, 'cable-geo.json')

        self.geo = None
        self.lps = dict() # {lp_id: {'country': country, 'geo': geo}}
        self.country = set()
        self.owners = set()
        
        self.query_dis = -1.0
    
    def init_cable(self):
        # Load Geo of Cable
        self._load_cable_geo()

        # Load Cable Landing-Points
        self._load_lp_id()

        # Load Landing-Points Geo
        self._load_lp_geo()

        # Build Cable
        self._build_cable()
    
    def query_straight_conn(self, lp_id):
        if lp_id not in self.lps.keys():
            print(f"[ERROR] No Landing-Point {lp_id}")
            return []
        visited = set()
        straight_conn_lp_id = set()
        q = queue.Queue()
        q.put(self.lps[lp_id]["node"])
        if self.lps[lp_id]["node"] == None:
            return []
        visited.add(self.lps[lp_id]["node"].format_coord())
        while not q.empty():
            node = q.get()
            for neighbor in node.neighbors:
                if neighbor.format_coord() in visited:
                    continue
                if neighbor.lp:
                    straight_conn_lp_id.add(neighbor.lp_id)
                    continue
                q.put(neighbor)
                visited.add(neighbor.format_coord())
        return straight_conn_lp_id
    
    def _dfs(self, begin_lp_id, current_node, visited, target_lp_id, current_leng):
        # print(current_leng, current_node.geo)
        if current_node.lp and current_node.lp_id != begin_lp_id:
            if current_node.lp_id == target_lp_id:
                self.query_dis = current_leng
            return visited
        for neighbor in current_node.neighbors:
            if neighbor.format_coord() in visited:
                # print('visited')
                continue
            visited.add(neighbor.format_coord())
            dis = geodesic(tuple(reversed(current_node.geo)), tuple(reversed(neighbor.geo))).km
            current_leng += dis
            visited = self._dfs(begin_lp_id, neighbor, visited, target_lp_id, current_leng)
            current_leng -= dis
        return visited

    def query_lps_distance(self, lp_id1, lp_id2):
        self.query_dis = -1.0
        if lp_id1 not in self.lps.keys() or lp_id2 not in self.lps.keys():
            print(f"[ERROR] No Landing-Point {lp_id1}, {lp_id2}")
            return self.query_dis
        self.query_dis = 0
        visited = set()
        self._dfs(lp_id1, self.lps[lp_id1]["node"], visited, lp_id2, 0)
        return self.query_dis

    def _load_cable_geo(self):
        open_file = open(self.CABLE_GEO_PATH, 'r')
        data = json.load(open_file)
        open_file.close()

        for item in data["features"]:
            if item["properties"]["id"] == self.cable_id:
                self.geo = item["geometry"]["coordinates"]
                break
    
    def _load_lp_id(self):
        file_list = os.listdir(self.CABLE_DIR)
        for file_name in file_list:
            if file_name in ['cable-geo.json', 'all.json'] or file_name.startswith('.'):
                continue
            file_path = os.path.join(self.CABLE_DIR, file_name)
            open_file = open(file_path, 'r')
            data = json.load(open_file)
            open_file.close()
            if data["id"] != self.cable_id:
                continue

            self.owners = data["owners"].split(", ")
            self.rfs = data['rfs_year']
            
            for lp in data["landing_points"]:
                self.lps[lp["id"]] = {
                    "country": lp["country"],
                    "geo": [],
                    "node": None
                }
                self.country.add(lp["country"])
            break
    
    def _load_lp_geo(self):
        open_file = open(self.LP_GEO_PATH, 'r')
        data = json.load(open_file)
        open_file.close()

        for item in data["features"]:
            if item["properties"]["id"] in self.lps.keys():
                self.lps[item["properties"]["id"]]["geo"] = item["geometry"]["coordinates"]
    
    def _build_cable(self):
        nodes = []
        for line in self.geo:
            current_node = None
            for point in line:
                node = SubmarineCableNode(point)
                ex, _ = node.existed(nodes)
                if ex:
                    node = _
                else:
                    nodes.append(node)
                if current_node != None:
                    current_node.neighbors.append(node)
                    node.neighbors.append(current_node)
                current_node = node
        
        for node in nodes:
            is_lp, lp_id = node.is_lp(self.lps)
            if is_lp:
                node.lp = True
                node.lp_id = lp_id
                node.lp_country = self.lps[lp_id]["country"]
                self.lps[lp_id]["node"] = node

    


