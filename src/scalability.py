"""
Runs algorithms and prints time statistics for scalability discussions.
"""
from time import perf_counter
from graph import HistoryGraph
from joblib import Parallel, delayed
from util import find_leaving_developers, get_dataset_path, get_exp_name, project_list


@delayed
def scalability_experiment_rq1_rq3(project_name, method_name):
    """
    First, create a graph (with default parameters) for the initial window, then slide
    the window day by day. While sliding, run the given method (`method_name`) for each
    day and keep some statistics, and return them.

    This method is for RQ1 and RQ3 because the given method can't have any
    parameters in this setup.

    Parameters
    ----------
    project_name (str):
        Name of the project.

    method name (str):
        Name of the method to run in the experiment. It have to match one of the
        methods defined in graph.HistoryGraph. Also, the given method cannot have
        any paramaters. For example, "get_connectors".

    Returns
    -------
    tuple:
        Tuple of experiment name, node statistics, average number of nodes, edge
        statistics, average number of edges, average time taken and total number
        of iterations.
    """
    experiment_name = get_exp_name(project_name)
    dataset_path = get_dataset_path(project_name)

    G = HistoryGraph(dataset_path)

    # Start iterations
    num_iters = 0
    total_num_nodes = 0
    total_num_edges = 0
    node_stat = {"Developer": 0, "Issue": 0, "ChangeSet": 0, "File": 0}
    edge_stat = {"commit": 0, "include": 0, "link": 0}
    time_taken = 0
    while True:
        num_iters += 1
        for node_type in node_stat:
            node_stat[node_type] += len(G._filter_nodes_by_kind(node_type))

        for edge_type in edge_stat:
            edge_stat[edge_type] += len(G._filter_edges_by_kind(edge_type))

        total_num_nodes += G.get_num_nodes()
        total_num_edges += G.get_num_edges()

        t_start = perf_counter()
        eval("G.{}()".format(method_name))
        t_end = perf_counter()
        time_taken += t_end - t_start

        if not G.forward_graph_one_day():
            break

    for node_type in node_stat:
        node_stat[node_type] = round(node_stat[node_type] / num_iters)

    for edge_type in edge_stat:
        edge_stat[edge_type] = round(edge_stat[edge_type] / num_iters)

    avg_num_nodes = round(total_num_nodes / num_iters)
    avg_num_edges = round(total_num_edges / num_iters)
    avg_time_taken = time_taken / num_iters

    return (
        experiment_name,
        node_stat,
        avg_num_nodes,
        edge_stat,
        avg_num_edges,
        avg_time_taken,
        num_iters,
    )


@delayed
def scalability_experiment_rq2(project_name):
    """
    First, find leaving developers, then create a graph (with default parameters) and
    find replacements for leaving developers. At the same time keep some statistics,
    and return them.

    Parameters
    ----------
    project_name (str):
        Name of the project.

    Returns
    -------
    tuple:
        Tuple of experiment name, node statistics, average number of nodes, edge
        statistics, average number of edges, average time taken and total number
        of recommended replacements.
    """
    experiment_name = get_exp_name(project_name)
    dataset_path = get_dataset_path(project_name)

    G = HistoryGraph(dataset_path)
    date_to_leaving_developers = find_leaving_developers(G)

    # Start iterations
    num_leaving_developers = 0
    total_num_nodes = 0
    total_num_edges = 0
    node_stat = {"Developer": 0, "Issue": 0, "ChangeSet": 0, "File": 0}
    edge_stat = {"commit": 0, "include": 0, "link": 0}
    time_taken = 0
    for date, leaving_developers in date_to_leaving_developers.items():
        G.forward_until(date)
        for leaving_developer in leaving_developers:
            num_leaving_developers += 1

            for node_type in node_stat:
                node_stat[node_type] += len(G._filter_nodes_by_kind(node_type))

            for edge_type in edge_stat:
                edge_stat[edge_type] += len(G._filter_edges_by_kind(edge_type))

            total_num_nodes += G.get_num_nodes()
            total_num_edges += G.get_num_edges()

            t_start = perf_counter()
            G.find_replacement(leaving_developer)
            t_end = perf_counter()
            time_taken += t_end - t_start

    for node_type in node_stat:
        node_stat[node_type] = round(node_stat[node_type] / num_leaving_developers)

    for edge_type in edge_stat:
        edge_stat[edge_type] = round(edge_stat[edge_type] / num_leaving_developers)

    avg_num_nodes = round(total_num_nodes / num_leaving_developers)
    avg_num_edges = round(total_num_edges / num_leaving_developers)
    avg_time_taken = time_taken / num_leaving_developers

    return (
        experiment_name,
        node_stat,
        avg_num_nodes,
        edge_stat,
        avg_num_edges,
        avg_time_taken,
        num_leaving_developers,
    )


def run_experiment_rq1_rq3(method_name):
    """
    Wrapper for RQ1 and RQ3 experiements.
    """
    print("Running experiments for '{}'. It might take a while.".format(method_name))

    results = Parallel(n_jobs=-1, verbose=10)(
        scalability_experiment_rq1_rq3(pname, method_name) for pname in project_list
    )

    print("\n\nMethod: {}\n".format(method_name))
    print_results(results)


def run_experiment_rq2():
    """
    Wrapper for RQ2 experiement.
    """
    print("Running experiments for 'find_replacements'. It might take a while.")

    results = Parallel(n_jobs=-1, verbose=10)(
        scalability_experiment_rq2(project_name) for project_name in project_list
    )

    print("\n\nMethod: find_replacements\n")
    print_results(results)


def print_results(res):
    """
    Print the given result in a readable format.
    """
    print("Experiment Name\t\t\t| Avg. num. nodes\t| Node Stats ")
    for exp_name, node_stat, avg_num_nodes, _, _, _, _ in res:
        print("{:<32}{:<5}\t\t\t{}".format(exp_name, avg_num_nodes, node_stat))

    print("\nExperiment Name\t\t\t| Avg. num. edges\t| Edge Stats ")
    for exp_name, _, _, edge_stat, avg_num_edges, _, _ in res:
        print("{:<32}{:<5}\t\t\t{}".format(exp_name, avg_num_edges, edge_stat))

    print("\nExperiment Name\t\t\t| Avg. time taken (sec)\t| Num. iterations")
    for exp_name, _, _, _, _, avg_time_taken, num_iters in res:
        print("{:<32}{:.4f}\t\t\t{}".format(exp_name, avg_time_taken, num_iters))

    print()


if __name__ == "__main__":
    run_experiment_rq2()

    for method in ["get_jacks", "get_mavens", "get_connectors", "balanced_or_hero"]:
        run_experiment_rq1_rq3(method)
