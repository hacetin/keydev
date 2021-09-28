"""
Get results in parallel and dump into pickle files.
"""

from graph import HistoryGraph
from util import (
    dump_results,
    get_exp_name,
    print_log,
    find_leaving_developers,
    get_dataset_path,
    project_list,
    sws_list,
)
from joblib import Parallel, delayed
from datetime import datetime


@delayed
def run_experiment(experiment_name, dataset_path, sliding_window_size):
    """
    Run experiment with default parameters and export results into a pickle file.
    First, create a graph for the inital window, then slide that window day by day.
    Find developers, mavens, connectors, jacks and knowledge distribution labels for
    each iteration (day). Also, find replacements if any developers leave the project.

    Parameters
    ----------
    experiment_name (str):
        Name of the experiment.

    dataset_path (str):
        Dataset path to read data.

    sliding_window_size (int):
        Number of days included to the artifact graph.
    """
    G = HistoryGraph(dataset_path, sliding_window_size)

    date_to_leaving_developers = find_leaving_developers(G)

    log_path = "logs/{}.log".format(experiment_name)
    print_log(
        "Started (Total iterations: {}).\n".format(G.get_num_iterations()),
        log_path,
        mode="w",
    )

    start = datetime.now()
    # Start iterations
    date_to_results = {}
    step = 0
    while True:
        step += 1

        date = G.get_last_included_date()
        date_to_results[date] = {
            "developers": G.get_developers(),
            "top_committers": G.get_top_committers(),
            "jacks": G.get_jacks(),
            "mavens": G.get_mavens(),
            "connectors": G.get_connectors(),
            "last_jack": G.find_last_sig_jack(),
            "last_maven": G.find_last_sig_maven(),
            "last_connector": G.find_last_sig_connector(),
            "num_files": G.get_num_files_in_project(),
            "num_reachable_files": G.get_num_reachable_files(),
            "num_rare_files": G.get_num_rare_files(),
            "balanced_or_hero": G.balanced_or_hero(),
            "replacements": {
                dev: G.find_replacement(dev)
                for dev in date_to_leaving_developers.get(date, [])
            },
        }

        print_log("{} -> {} nodes\n".format(step, G.get_num_nodes()), log_path)

        if not G.forward_graph_one_day():
            break

    end = datetime.now()
    print_log("Ended.(Time taken: {})\n".format(end - start), log_path)

    dump_results(experiment_name, date_to_results)
    print_log("Exported results to 'results/{}.pkl'".format(experiment_name), log_path)


if __name__ == "__main__":
    # dl  -> distance limit (using default (10) in graph.HistoryGraph)
    # nfl -> number of files limit (using default (50) in graph.HistoryGraph)
    # sws -> sliding window size is
    experiments = []
    for project_name in project_list:
        dataset_path = get_dataset_path(project_name)
        for sws in sws_list:
            exp_name = get_exp_name(project_name, sws=sws)
            experiments.append((exp_name, dataset_path, sws))

    # Run all in parallel using all CPUs.
    Parallel(n_jobs=-1, verbose=10)(run_experiment(*params) for params in experiments)
