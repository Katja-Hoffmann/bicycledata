# definiere generelle Variablen
place_name = "Munich, Bavaria, Germany"

def transform(df):
    import pandas as pd
    from _datetime import datetime

    listofbikes = df["bike"].unique()

    # Auteilung der Spalte timestamp in Datum und Uhrzeit
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df['time'] = pd.to_datetime(df['timestamp']).dt.time
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour

    routes = []

    for bike in listofbikes:
        bike_df = df.loc[df['bike'] == bike]
        bike_doc = bike_df.to_dict(orient='records')

        try:
            oldlon = bike_doc[0]["lon"]
            oldlat = bike_doc[0]["lat"]
            laststamp = bike_doc[0]['timestamp']
        except:
            continue

        for row in bike_doc:
            currentstamp = row["timestamp"]

            # Check if bike has moved
            if oldlon != row["lon"] or oldlat != row["lat"]:
                route = {}
                route["bike"] = bike
                route["date"] = str(row["date"])
                route["starttime"] = laststamp
                route["endtime"] = currentstamp
                route["startlon"] = oldlon
                route["startlat"] = oldlat
                route["endlon"] = row["lon"]
                route["endlat"] = row["lat"]
                routes.append(route)

            oldlon = row["lon"]
            oldlat = row["lat"]

            laststamp = currentstamp

    import networkx as nx
    import osmnx as ox
    ox.config(use_cache=True, log_console=True)


    #G = ox.graph_from_place(place_name, network_type='bike')
    G = ox.load_graphml("muenchen.graphml")

    printroutes = []
    ins = len(routes)
    cnt = 0
    for route in routes:
        try:
            orig_node = ox.get_nearest_node(G, (route["startlat"], route["startlon"]))
            dest_node = ox.get_nearest_node(G, (route["endlat"], route["endlon"]))
            route1 = nx.shortest_path(G, orig_node, dest_node, weight='length')
            printroutes.append(route1)
            # cnt += 1
            # if (cnt % 100) == 0:
            #     print(str(cnt) + " | " + str(ins))
            route["route"] = route1
        except:
            continue

    #     ox.plot_graph_routes(G, listofroutes, fig_height=20, node_size=1, route_linewidth=1,
    #                          route_alpha=0.4, orig_dest_node_size=10, orig_dest_node_color='r',
    #                          node_alpha=0.2, save=True, filename="graphbikes", file_format="png")

    streetlist = []
    for route in routes:
        try:
            streets = []
            lastnode = route["route"][0]
            lastname = "empty"
            for node in route["route"]:
                if node == lastnode:
                    continue
                for d in G.get_edge_data(lastnode, node).values():
                    if 'name' in d:
                        if type(d['name']) == str and d['name'] not in lastname:
                            streetlist.append(d['name'])
                            streets.append(d['name'])
                            lastname = d['name']
                lastnode = node
            route["streetlist"] = streets
        except:
            continue

    import numpy
    for row in routes:
        if 'route' not in row.keys():
            routes.remove(row)
            continue
        for key in row:
            if isinstance(row[key], numpy.int64): row[key] = int(row[key])

    return routes

def pltfromdoc(doc):
    import networkx as nx
    import osmnx as ox
    ox.config(use_cache=True, log_console=True)
    listofroutes = []
    for row in doc:
        listofroutes.append(row["route"])
    #G = ox.graph_from_place(place_name, network_type='bike')
    G = ox.load_graphml("muenchen.graphml")
    ox.plot_graph_routes(G, listofroutes, node_size=1, route_linewidth=1,
                         route_alpha=0.4, orig_dest_size=10, node_color='r',
                         node_alpha=0.2, save=True, filepath="images/graphbikes.png", figsize=(15,15))


def heatfromdoc(doc):
    import networkx as nx
    import osmnx as ox
    from folium import Map
    import folium.plugins as plugins

    ox.config(use_cache=True, log_console=True)
    #G = ox.graph_from_place(place_name, network_type='bike')
    G = ox.load_graphml("muenchen.graphml")
    gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)

    timestamps = []
    for row in doc:
        timestamps.append(row["starttime"])
    timestamps = list(sorted(timestamps))

    datapoints = []
    cnt = 0
    timeindex = []
    points = []
    for time in timestamps:
        for route in doc:
            try:
                if route["starttime"] == time:
                    for node in route["route"]:
                        point = []
                        nodepoint = gdf_nodes.loc[node]
                        point = [nodepoint["y"], nodepoint["x"], 1]
                        points.append(point)

            except:
                continue
        if cnt == 6: cnt = 0
        if points != [] and cnt == 0:
            datapoints.append(points)
            timeindex.append(str(time))
            points = []
        cnt += 1
    m = Map([52.521661, 13.413544], tiles="cartodbpositron", zoom_start=13)

    hm = plugins.HeatMapWithTime(
        datapoints,
        index=timeindex,
        auto_play=True,
        max_opacity=0.5
        , radius=8
        , use_local_extrema=True
    )

    hm.add_to(m)
    m.save('index.html')
    #return m


def pltStreetCount(doc):
    # libraries
    import numpy as np
    import matplotlib.pyplot as plt

    streetlist = []
    for row in doc:
        for street in row["streetlist"]:
            streetlist.append(street)

    from collections import Counter
    cnt = Counter(streetlist).most_common(10)
    # print(Counter(streetlist).most_common(10))

    height = []
    bars = []
    for key, value in cnt:
        height.append(value)
        bars.append(key)
    y_pos = np.arange(len(bars))

    # Create horizontal bars
    plt.barh(y_pos, height)

    # Create names on the y-axis
    plt.yticks(y_pos, bars)

    bottom, top = plt.ylim()
    plt.ylim(top, bottom) 
    # Show graphic
    plt.show()

