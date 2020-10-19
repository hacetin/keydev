from time import perf_counter
from graph import HistoryGraph
from joblib import Parallel, delayed, cpu_count


def run_experiment(experiment_name, dataset_path):
    """
    Run experiment with default parameters and export results into a pickle file.
    First, create a graph for the inital window, then slide that window day by day.
    Find developers, mavens, connectors and jacks for each iteration.

    Parameters
    ----------
    experiment_name (str):
        Name of the experiment.

    dataset_path (str):
        Dataset path to read data.
    """

    G = HistoryGraph(dataset_path)

    # Start iterations
    result = {}
    i = 0
    total_num_nodes = 0
    total_num_edges = 0
    node_stat = {"Developer": 0, "Issue": 0, "ChangeSet": 0, "File": 0}
    edge_stat = {"commit": 0, "include": 0, "link": 0}
    time_taken = 0
    while True:
        i += 1
        for node_type in node_stat:
            node_stat[node_type] += len(G._filter_nodes_by_kind(node_type))

        for edge_type in edge_stat:
            edge_stat[edge_type] += len(G._filter_edges_by_kind(edge_type))

        total_num_nodes += G.get_num_nodes()
        total_num_edges += G.get_num_edges()

        t0 = perf_counter()
        G.get_jacks()
        G.get_mavens()
        G.get_connectors()
        t1 = perf_counter()
        time_taken += t1 - t0

        if not G.forward_graph_one_day():
            break

    for node_type in node_stat:
        node_stat[node_type] = round(node_stat[node_type] / i)

    for edge_type in edge_stat:
        edge_stat[edge_type] = round(edge_stat[edge_type] / i)

    avg_num_nodes = round(total_num_nodes / i)
    avg_num_edges = round(total_num_edges / i)
    avg_time_taken = time_taken / i

    return (
        experiment_name,
        node_stat,
        avg_num_nodes,
        edge_stat,
        avg_num_edges,
        avg_time_taken,
        i,
    )


# Experiment names and dataset paths
# dl10   -> distance limit is 10
# nfl50  -> number of files limit is 50
# sws365 -> sliding window size is 365
experiments = [
    ("hadoop_dl10_nfl50_sws365", "data/hadoop_change_sets.json"),
    ("hive_dl10_nfl50_sws365", "data/hive_change_sets.json"),
    ("pig_dl10_nfl50_sws365", "data/pig_change_sets.json"),
    ("hbase_dl10_nfl50_sws365", "data/hbase_change_sets.json"),
    ("derby_dl10_nfl50_sws365", "data/derby_change_sets.json"),
    ("zookeeper_dl10_nfl50_sws365", "data/zookeeper_change_sets.json"),
]

# Run all in parallel using all CPUs.
res = Parallel(n_jobs=-1, verbose=10)(
    delayed(run_experiment)(experiment_name, dataset_path)
    for experiment_name, dataset_path in experiments
)

print("Experiment Name\t\t\t| Avg. num. nodes\t| Node Stats ")
for exp_name, node_stat, avg_num_nodes, _, _, _, _ in res:
    print("{:<32}{:<5}\t\t\t{}".format(exp_name, avg_num_nodes, node_stat))

print("\nExperiment Name\t\t\t| Avg. num. edges\t| Edge Stats ")
for exp_name, _, _, edge_stat, avg_num_edges, _, _ in res:
    print("{:<32}{:<5}\t\t\t{}".format(exp_name, avg_num_edges, edge_stat))

print("\nExperiment Name\t\t\t| Avg. time taken\t| Num. iterations")
for exp_name, _, _, _, _, avg_time_taken, num_iters in res:
    print("{:<32}{:.2f}\t\t\t{}".format(exp_name, avg_time_taken, num_iters))
