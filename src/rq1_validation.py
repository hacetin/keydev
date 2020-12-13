"""
Generates the number in result tables for RQ1.
"""
from joblib import Parallel, delayed
import random
from util import get_exp_name, load_results, project_list, sws_list
from extract_commenters import generate_date_to_top_commenters
from collections import defaultdict


def accuracy(set1, set2):
    """
    Calculate intersection ratio of set1 and set2 over set1.
    For example, return 0.75 if "set1={1,2,3,4}" and "set2={1,2,3,5,6,7}".

    Parameters
    set1 (set):
        Set of items.

    set2 (set):
        Set of items.

    Returns
    -------
    float:
        Intersection ratio of set1 and set2 over set1.
    """
    if len(set1) == 0:
        return 0  # To not boost accuracy

    return len(set1.intersection(set2)) / len(set1)


def print_table(table):
    """
    Print topk accuracy table in a readable way.

    Parameters
    ----------
    table (dict):
        Mapping from row and column indexes to the values of the cells.
        For example, "{(0,0): 0.65,  (0,1): 0.78}"
    """
    rows = sorted(set(i for i, _ in table))
    cols = sorted(set(j for _, j in table))

    print("\t" + "\t".join(str(x) for x in cols))
    for i in rows:
        row_text = str(i)
        for j in cols:
            if (i, j) in table:
                row_text += "\t{:.2f}".format(table[(i, j)] * 100)
            else:
                row_text += "\t-"
        print(row_text)


def topk_table(date_to_key_developers, date_to_top_commenters):
    """
    Calculate accuracies which are required for topk table.

    Parameters
    ----------
    date_to_predicted_key_developers (dict):
        Mapping from dates to list of predicted key developers.

    date_to_correct_key_developers (dict):
        Mapping from dates to list of correct (true) key developers (like ground truth).

    Returns
    -------
    dict:
        Mapping from row and column indexes to the values of the cells.
        For example, "{(0,0): 0.65,  (0,1): 0.78}"
    """
    kvalues = [1, 3, 5, 10]
    accs = {(k1, k2): [] for k1 in kvalues for k2 in kvalues if k1 <= k2}
    for date, key_developers in date_to_key_developers.items():
        top_commenters = list(date_to_top_commenters[date])

        for k1, k2 in accs:
            acc = accuracy(set(top_commenters[:k1]), set(key_developers[:k2]))
            accs[(k1, k2)].append(acc)

    avg_accs = {c: sum(values) / len(values) for c, values in accs.items()}
    return avg_accs


def generate_date_to_intersection(date_to_results):
    """
    Generate a mapping from date to intersection developers of that date.

    Parameters
    ----------
    date_to_results (dict):
        Mapping from date to results. Results have to include "jacks", "mavens"
        and "connectors" categories at the same time.

    Returns
    -------
    dict:
        Mapping from date to intersection developers that date. Intersection developers
        means the developers who are jack, maven and connector at the same time.
    """
    date_to_intersection = {}
    for date, results in date_to_results.items():
        intersection_developers = set.intersection(
            set(results["jacks"].keys()),
            set(results["mavens"].keys()),
            set(results["connectors"].keys()),
        )

        # Let's sort the intersection developers according to jack score
        # Jacks are already sorted
        sorted_intersection_developers = {
            dev: score
            for dev, score in results["jacks"].items()
            if dev in intersection_developers
        }
        date_to_intersection[date] = sorted_intersection_developers

    return date_to_intersection


def validation(date_to_key_developers, date_to_top_commenters, date_to_developers):
    """
    Perform validation by considering the top commenters as the ground truth (actually,
    it is a pseudo ground truth). Also, perform Monte Carlo simulation.

    Then, print a topk accuracy table for the given key developers and another topk
    accuracy table for monte carlo simulation.

    Parameters
    ----------
    date_to_key_developers (dict):
        Mapping from date to key developers (one type of key developers such as "jacks"
        or "intersection") in the sliding window ending that date.

    date_to_top_commenters (dict):
        Mapping from date to top commenters in the sliding window ending that date.

    date_to_developers (dict):
        Mapping from date to all developers in the sliding window ending that date.

    Returns
    -------
    dict:
        Accuracy table of our approaches. Mapping from row and column indexes to the
        values of the cells. For example, "{(0,0): 0.65,  (0,1): 0.78}"

    dict:
        Accuracy table of Monte Carlo simulation. Mapping from row and column indexes
        to the values of the cells. For example, "{(0,0): 0.65,  (0,1): 0.78}"
    """
    ## OUR APPROACH
    our_acc_table = topk_table(date_to_key_developers, date_to_top_commenters)

    ## MONTE CARLO SIMULATION
    num_simulations = 1000
    monte_carlo_acc_tables = []
    for _ in range(num_simulations):
        # Generate random key developers
        date_to_random_developers = {}
        for date, key_developers in date_to_key_developers.items():
            random_developers = random.sample(
                date_to_developers[date], len(key_developers)
            )
            date_to_random_developers[date] = random_developers

        acc_table = topk_table(date_to_random_developers, date_to_top_commenters)
        monte_carlo_acc_tables.append(acc_table)

    # Find the average accuracy of all simulations

    # First, find the sum of the accuracies for each table cell
    monte_carlo_sum_acc_table = defaultdict(lambda: 0)
    for acc_table in monte_carlo_acc_tables:
        for cell, score in acc_table.items():
            monte_carlo_sum_acc_table[cell] += score

    # Then, divide the sums to number of simulations to find average of the accuracies
    # for each table cell
    monte_carlo_avg_acc_table = {
        k: v / num_simulations for k, v in monte_carlo_sum_acc_table.items()
    }

    return our_acc_table, monte_carlo_avg_acc_table


@delayed
def validation_wrapper(project_name, sws):
    """
    Wrapper to run validation method. First, find intersection developers, then
    run the validation.

    Parameters
    ----------
    project_name (str):
        Name of the project.

    sws (int):
        Sliding_window_size.

    Returns
    -------
    str:
        NAme of the experiment

    dict:
        Mapping from each category to tuple of its topk tables.
    """
    exp_name = get_exp_name(project_name, sws=sws)
    date_to_results = load_results(exp_name)

    # Add intersection to results
    date_to_intersection = generate_date_to_intersection(date_to_results)
    for date in date_to_results:
        date_to_results[date]["intersection"] = date_to_intersection[date]

    date_to_top_commenters = generate_date_to_top_commenters(project_name, sws)
    date_to_top_commenters = {
        date: list(top_commenters.keys())
        for date, top_commenters in date_to_top_commenters.items()
    }

    date_to_developers = {
        date: results["developers"] for date, results in date_to_results.items()
    }

    res_dict = {"jacks": (), "intersection": ()}
    for category in res_dict:
        date_to_key_developers = {
            date: list(results[category].keys())
            for date, results in date_to_results.items()
        }

        acc_table, monte_carlo_avg_acc_table = validation(
            date_to_key_developers, date_to_top_commenters, date_to_developers
        )

        avg_improvement = (
            sum(
                acc1 / acc2
                for acc1, acc2 in zip(
                    acc_table.values(), monte_carlo_avg_acc_table.values()
                )
            )
            / len(acc_table)
        ) * 100 - 100

        res_dict[category] = (acc_table, monte_carlo_avg_acc_table, avg_improvement)

    return exp_name, res_dict


if __name__ == "__main__":
    experiments = []
    for project_name in project_list:
        for sliding_window_size in sws_list:
            experiments.append((project_name, sliding_window_size))

    outputs = Parallel(n_jobs=-1, verbose=10)(
        validation_wrapper(*params) for params in experiments
    )

    for exp_name, res_dict in outputs:
        print("\n\n-> {}".format(exp_name))
        for category, (
            our_acc_table,
            monte_carlo_avg_acc_table,
            avg_improvement,
        ) in res_dict.items():
            print("--> {}".format(category))
            print("Our Approach - Top Commenters")
            print_table(our_acc_table)
            print("Monte Carlo Simulation - Top Commenters")
            print_table(monte_carlo_avg_acc_table)
            print("Average Improvement (Gain): {:.2f}%".format(avg_improvement))
