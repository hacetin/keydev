"""
Helps reading results of the experiments and precalculates some fields. 
"""

import pickle
import os

dirname = os.path.dirname(__file__)
results_path = os.path.join(dirname, "./../results")


def dev2scores_over_time(results):
    """
    Separate "results" by "category" (jacks, mavens and connectors).

    Returns
    -------
    dict:
        Mapping from category to all category history of every developer. If developer
        doesn't have any score in a date, we assign None to score of the developer.
        Besides developer scores, it includes average score for each date.
    """
    all_developers = set()
    for date in results:
        all_developers.update(results[date]["developers"])

    categories = ["jacks", "mavens", "connectors"]

    # Initialize the structure
    score_history = {}
    for category in categories:
        score_history[category] = {}
        for dev in all_developers:
            score_history[category][dev] = []
        score_history[category]["AVERAGE"] = []

    # Then, fill it
    for category in categories:
        for date in results:
            summ = 0
            counter = 0
            for dev in all_developers:
                score = results[date][category].get(dev, None)
                score_history[category][dev].append(score)
                if score:
                    summ += score
                    counter += 1

            average_score = 0
            if counter:
                average_score = summ / counter

            score_history[category]["AVERAGE"].append(average_score)

    return score_history


def get_experiment_names():
    """
    Detect experiments and return their names.

    Returns
    -------
    list:
        Names of the experiments.
    """
    exp_names = [
        fname[:-4] for fname in os.listdir(results_path) if fname.endswith(".pkl")
    ]

    # Sort as in the paper -> hadoop, hive, pig, hbase, derby, zookeeper
    sort_order = ["ha", "hi", "p", "hb", "d", "z"]

    exp_names = [
        exp_name
        for start in sort_order
        for exp_name in sorted(exp_names)
        if exp_name.startswith(start)
    ]

    return exp_names


def read_data(experiment_name):
    """
    Read results of the given experiment. Also, preprocess to find jacks, mavens
    and connectors over time.

    Parameters
    ----------
    experiment_name (str):
        Name of the experiment to read its data.

    Returns
    -------
    dict:
        Mapping from date to results for that date.

    list:
        All dates in results. This is just to keep all dates as a separate list.

    dict:
        Mapping from categories to their score history over time.
    """
    with open("{}/{}.pkl".format(results_path, experiment_name), "rb") as f:
        results = pickle.load(f)

    score_history = dev2scores_over_time(results)
    dates = list(results.keys())

    return results, dates, score_history


def all_exps():
    """
    Read all experiment results. Also, precalculate some useful fields like jack score
    of all developers over time.

    Returns
    -------
    dict:
        Mapping from experiment names to their results.
    """
    experiments_to_results = {}
    experiment_names = get_experiment_names()
    for exp in experiment_names:
        results, dates, score_history = read_data(exp)
        experiments_to_results[exp] = {
            "results": results,
            "dates": dates,
            "score_history": score_history,
        }

    return experiments_to_results


if __name__ == "__main__":
    # For debugging
    all_exps()
