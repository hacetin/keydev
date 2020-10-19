from data_manager import DataManager, SlidingNotPossible
from util import sort_dict, highest_k, date_to_str
from collections import defaultdict


def generate_date_to_top_committers(project_name):
    """
    Generate a mapping from date to number of commits made until that date.

    TODO:
    Large change sets can be excluded.

    Parameters
    ----------
    project_name (str):
        Name of the project

    Returns
    --------
    dict:
        Mapping from date to top committers and their numbers of commits in the sliding
        window ending that date.
    """

    data_manager = DataManager("data/{}_change_sets.json".format(project_name), 365)

    # Get initial change sets to add and remove
    change_sets_add = data_manager.get_initial_window()
    change_sets_remove = {}
    top_committers = defaultdict(lambda: 0)

    date_to_top_committers = {}
    while True:
        # Add change sets
        for change_set in change_sets_add:
            top_committers[change_set.author] += 1

        # Remove change sets
        for change_set in change_sets_remove:
            author = change_set.author
            top_committers[author] -= 1
            if top_committers[author] <= 0:
                del top_committers[author]

        date = data_manager.get_last_included_date()
        date_to_top_committers[date] = sort_dict(
            top_committers, by_value=True, reverse=True
        )

        try:
            change_sets_add, change_sets_remove = data_manager.forward_one_day()
        except SlidingNotPossible:
            break

    return date_to_top_committers


if __name__ == "__main__":
    # Lets extract top committers:
    k = 10
    for project_name in ["pig", "hive", "hadoop", "hbase", "derby", "zookeeper"]:
        text = ""
        date_to_top_committers = generate_date_to_top_committers(project_name)
        for date, dev_to_commit_count in date_to_top_committers.items():
            topk_committers = highest_k(dev_to_commit_count, k)
            text += date_to_str(date) + "," + ",".join(topk_committers) + "\n"

        with open("data/{}_top_committers.csv".format(project_name), "w") as f:
            f.write(text)
