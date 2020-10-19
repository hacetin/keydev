from graph import HistoryGraph
from data_manager import DataManager
from datetime import timedelta
from util import print_log
import random
from joblib import Parallel, delayed
import pickle
from functools import lru_cache


@delayed
def validation(project_name, sliding_window_size, check_days, max_k, random_val):
    dataset_path = "data/{}_change_sets.json".format(project_name)

    dm = DataManager(dataset_path, None)
    G = HistoryGraph(dataset_path, sliding_window_size)

    ranks = {key: [] for key in check_days}
    our_results = load_results(project_name, sliding_window_size)
    for date, results in our_results.items():
        if not results["replacements"]:
            continue

        # update graph
        G.forward_until(date)

        for leaving_dev, recommended_devs in results["replacements"].items():
            if not recommended_devs:
                continue

            if random_val:
                # Randomly select "max_k" developers
                other_devs = G.get_developers()
                other_devs.remove(leaving_dev)
                recommended_devs = random.sample(other_devs, max_k)
            else:
                # Convert dictionary keys to list and get first "max_k" items
                recommended_devs = [*recommended_devs][:max_k]

            leaving_dev_files = set(G.find_reachable_files(leaving_dev))

            for check_day in check_days:
                # get the change sets for the check days
                change_sets = dm.get_specific_window(
                    date + timedelta(days=1), date + timedelta(days=check_day)
                )
                rank = float("inf")
                for i, recommended_dev in enumerate(recommended_devs):
                    recommended_dev_files = set(G.find_reachable_files(recommended_dev))
                    target_files = leaving_dev_files - recommended_dev_files

                    if check_modification(change_sets, recommended_dev, target_files):
                        rank = i + 1
                        break

                ranks[check_day].append(rank)

    exp_name = "{}_sws{}".format(project_name, sliding_window_size)
    ret_items = [exp_name]

    for check_day in check_days:
        res = {}
        for k in range(1, max_k + 1):
            res["top{}".format(k)] = "{:.2f}".format(cal_accuracy(ranks[check_day], k))

        res["mrr"] = "{:.2f}".format(cal_mrr(ranks[check_day]))

        ret_items.append((check_day, res))
    return ret_items


def check_modification(change_sets, recommended_dev, target_files):
    """
    Check the given change sets. If `recommended_dev` change any of `target_files`,
    return True. Otherwise return False.

    Returns
    -------
    bool:
        True if `recommended_dev` change any of `target_files`. Otherwise, False.
    """

    for cs in change_sets:
        if cs.author != recommended_dev:
            continue

        for cc in cs.code_changes:
            if cc.change_type == "RENAME" and cc.old_file_path in target_files:
                target_files.remove(cc.old_file_path)
                target_files.add(cc.file_path)
            elif cc.change_type == "MODIFY" and cc.file_path in target_files:
                return True


def cal_accuracy(ranks, k):
    return 100 * sum(1 for rank in ranks if rank <= k) / len(ranks)


def cal_mrr(ranks):
    return 100 * sum(1 / rank for rank in ranks) / len(ranks)


# @lru_cache(maxsize=None)
def load_results(project_name, sliding_window_size):
    path = "results/{}_dl10_nfl50_sws{}.pkl".format(project_name, sliding_window_size)
    with open(path, "rb") as f:
        return pickle.load(f)


if __name__ == "__main__":
    experiments = []
    for project_name in ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]:
        for sliding_window_size in [180, 365]:
            experiments.append((project_name, sliding_window_size))

    max_k = 3
    check_days = (7, 30, 90)

    # OUR APPROACH
    res = Parallel(n_jobs=-1, verbose=10)(
        validation(*params, check_days, max_k, False) for params in experiments
    )

    print("\nOUR APPROACH\n")
    for res_item in res:
        for item in res_item:
            print(item)

    # RANDOM SELECTION
    num_sims = 100
    res = Parallel(n_jobs=-1, verbose=10)(
        validation(*params, check_days, max_k, True) for params in experiments
    )

    print("\n\nRANDOM SELECTION - {}\n".format(num_sims))
    for res_item in res:
        for item in res_item:
            print(item)
