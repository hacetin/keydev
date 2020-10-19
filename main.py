"""
Get results in parallel and dump into pickle files.
"""

from graph import HistoryGraph
from util import print_log
import pickle
from joblib import Parallel, delayed
from datetime import timedelta


def find_leaving_developers(G):
    """
    Find the leaving developers in the given dataset. If any existing developer
    disappear from the graph when sliding the window, we consider s/he leaved the
    project at his/her last contribution date.

    Parameters
    ----------
    G (graph.HistoryGraph): Graph to find leaving developers.

    Returns
    -------
    dict:
        Mapping from dates to leaving developers that day (last contribution day).
    """
    absence_limit = G._graph_range_in_days
    date_to_leaving_developers = {}
    prev_developers = set()
    while True:
        developers = set(G.get_developers())
        leaving_developers = prev_developers.difference(developers)
        prev_developers = developers
        if leaving_developers:
            date = G.get_last_included_date()
            leaving_day = date - timedelta(days=absence_limit)
            date_to_leaving_developers[leaving_day] = leaving_developers

        if not G.forward_graph_one_day():
            break

    return date_to_leaving_developers


def run_experiment(
    experiment_name, dataset_path, distance_limit, num_files_limit, sliding_window_size
):
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
    G = HistoryGraph(
        dataset_path=dataset_path,
        graph_range_in_days=sliding_window_size,
        distance_limit=distance_limit,
        num_files_limit=num_files_limit,
    )

    date_to_leaving_developers = find_leaving_developers(G)

    G = HistoryGraph(
        dataset_path=dataset_path,
        graph_range_in_days=sliding_window_size,
        distance_limit=distance_limit,
        num_files_limit=num_files_limit,
    )

    log_path = "logs/{}.log".format(experiment_name)
    print_log(
        "Started (Total iterations: {}).\n".format(G.get_num_iterations()),
        log_path,
        mode="w",
    )

    # Start iterations
    result = {}
    i = 0
    while True:
        i += 1

        date = G.get_last_included_date()
        result[date] = {
            "developers": G.get_developers(),
            "jacks": G.get_jacks(),
            "mavens": G.get_mavens(),
            "connectors": G.get_connectors(),
            "last_jack": G.find_last_sig_jack(),
            "last_maven": G.find_last_sig_maven(),
            "last_connector": G.find_last_sig_connector(),
            "num_files": G.get_num_files(),
            "num_reachable_files": G.get_num_reachable_files(),
            "num_rare_files": G.get_num_rare_files(),
            "balanced_or_hero": G.balanced_or_hero(),
            "replacements": {
                dev: G.find_replacement(dev)
                for dev in date_to_leaving_developers.get(date, [])
            },
        }

        print_log("{} -> {} nodes\n".format(i, G.get_num_nodes()), log_path)

        if not G.forward_graph_one_day():
            break

    print_log("Ended.\n", log_path)

    with open("results/{}.pkl".format(experiment_name), "wb") as f:
        pickle.dump(result, f)

    print_log("Exported results to 'results/{}.pkl'".format(experiment_name), log_path)


if __name__ == "__main__":
    # Experiment names and dataset paths
    # dl  -> distance limit
    # nfl -> number of files limit
    # sws -> sliding window size is
    experiments = []
    for pname in ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]:
        dataset_path = "data/{}_change_sets.json".format(pname)
        for dl in [10]:
            for nfl in [50]:
                for sws in [180, 365]:  # 180, 365
                    exp_name = "{}_dl{}_nfl{}_sws{}".format(pname, dl, nfl, sws)
                    experiments.append((exp_name, dataset_path, dl, nfl, sws))

    # Run all in parallel using all CPUs.
    Parallel(n_jobs=-1, verbose=10)(
        delayed(run_experiment)(*params) for params in experiments
    )
