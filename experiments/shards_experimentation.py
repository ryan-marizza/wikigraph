from wikigraph.shards import discover

if __name__ == "__main__":
    s = discover()
    wanted = "['p10p1400054']"
    shards = [s.as_dict() for s in discover() if not wanted or s.name in wanted]

    print(shards)