"""
Generates the number in result tables for RQ3.
"""
from util import get_exp_name, load_results, project_list, sws_list, alpha_list
from extract_committers import generate_date_to_top_committers
from math import ceil


def balanced_or_hero_pareto(commit_counts):
    """
    Check if the team is hero or balanced according to the given commit counts.
    If 20% of developers made the 80% or more of commits, return "hero".
    Otherwise, return "balanced".

    Parameters
    ----------
    commit_counts (list):
        Commit counts of developers.

    Returns
    -------
    str:
        "hero" or "balanced".
    """
    num_covered_devs = ceil(len(commit_counts) * 0.2)
    num_covered_commits = sum(sorted(commit_counts, reverse=True)[:num_covered_devs])

    if num_covered_commits / sum(commit_counts) >= 0.8:
        return "hero"

    return "balanced"


def balanced_or_hero_pareto_over_time(date_to_dev_to_commit_count):
    """
    Check if the team is hero or balanced for each date in the given dict.

    Parameters
    ----------
    commit_counts (dict):
        Mapping from date to developer commit count pairs.

    Returns
    -------
    dict:
        Mapping from date to "hero" or "balanced".
    """
    date2label = {}
    for date, dev_to_commit_count in date_to_dev_to_commit_count.items():
        date2label[date] = balanced_or_hero_pareto(dev_to_commit_count.values())

    return date2label


def accuracy(dict1, dict2):
    """
    Calculate accuracy for the dates which are in both dictionaries.

    Parameters
    ----------
    dict1 (dict):
        Mapping for date and labels.

    dict2 (dict):
        Mapping for date and labels.

    Returns
    -------
    float:
        Accuracy. In other words, intersection ratio of the labels for the dates which
        are in both dictionaries.
    """
    intersection_dates = set(dict1.keys()).intersection(dict2.keys())
    num_matches = sum(1 for date in intersection_dates if dict1[date] == dict2[date])
    accuracy = num_matches / len(intersection_dates)

    return accuracy


if __name__ == "__main__":
    for sws in sws_list:
        print("*** Sliding Window Size: {} ***\n".format(sws))
        for project_name in project_list:
            print(project_name)
            for alpha in alpha_list:
                print("Alpha: {}".format(alpha))
                our_results = load_results(get_exp_name(project_name, sws=sws))

                date_to_label_our = {
                    date: our_results[date]["balanced_or_hero_{}".format(alpha)]
                    for date in our_results
                    if our_results[date][
                        "balanced_or_hero_{}".format(alpha)
                    ]  # num of devs is not less than 3
                }

                date_to_dev_to_commit_counts = generate_date_to_top_committers(
                    project_name, sws
                )
                date_to_label_pareto = balanced_or_hero_pareto_over_time(
                    date_to_dev_to_commit_counts
                )

                num_hero_our = 0
                num_hero_pareto = 0
                for date in date_to_label_our:
                    if date_to_label_our[date] == "hero":
                        num_hero_our += 1
                    if date_to_label_pareto[date] == "hero":
                        num_hero_pareto += 1

                hero_ratio_our = 100 * num_hero_our / len(date_to_label_our)
                hero_ratio_pareto = 100 * num_hero_pareto / len(date_to_label_our)
                print(
                    "Our Approach:",
                    "{:.2f}% balanced and {:.2f}% hero".format(
                        100 - hero_ratio_our, hero_ratio_our
                    ),
                )
                print(
                    "Pareto:",
                    "{:.2f}% balanced and {:.2f}% hero".format(
                        100 - hero_ratio_pareto, hero_ratio_pareto
                    ),
                )
                print(
                    "Accuracy: {:.2f}%".format(
                        100 * accuracy(date_to_label_our, date_to_label_pareto)
                    )
                )
                print()
            print()
        print()
