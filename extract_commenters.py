"""
Extracts top commenters for each sliding window.
"""
from data_manager import DataManager, SlidingNotPossible
from preprocess import (
    pig_author_mapping,
    hive_author_mapping,
    hadoop_author_mapping,
    hbase_author_mapping,
    derby_author_mapping,
    zookeeper_author_mapping,
)
from util import (
    execute_db_query,
    sort_dict,
    highest_k,
    date_to_str,
    get_dataset_path,
    project_list,
    sws_list,
)
from collections import defaultdict

# Since all 3 projects belong to Apache community and they all use the same jira system,
# we combined the author mapping of all projects.
combined_author_mapping = {
    **pig_author_mapping,
    **hive_author_mapping,
    **hadoop_author_mapping,
    **hbase_author_mapping,
    **derby_author_mapping,
    **zookeeper_author_mapping,
    # After this point, there are new mappings that we found while
    # we were inspecting comment data, not from Git names.
    **{
        "linte": "alexandre linte",
        "aniket mokashi": "aniket namadeo mokashi",
        "@deprecated yi deng": "yi deng",
    },
}


# Some comments are generated automatically
# Following commenters are ignored
ignored_commenters = set(
    [
        "asf github bot",
        "asf subversion and git services",
        "jiraposter@reviews.apache.org",
        "hbase review board",
        "hadoop qa",
        "hudson",
        "hive qa",
        "phabricator",
        "noreply@reviews.facebook.net",
    ]
)


def generate_issue_to_commenters(project_name):
    """
    Generate a mapping from issue ids to commenters of the issues.

    Parameters
    ----------
    project_name (str):
        Name of the project.

    Returns
    --------
    dict:
        Mapping from issue ids to commenters of the issues.
    """
    query_results = execute_db_query(
        "data/{}.sqlite3".format(project_name),
        """
        SELECT issue_id, display_name
        FROM issue_comment
        """,
    )

    issue_to_commenters = defaultdict(list)
    for issue_id, commenter in query_results:
        # Clear whitespaces and make lower case.
        commenter = commenter.strip().lower()
        # Check ignore commenters
        if commenter in ignored_commenters:
            continue

        # Replace the commenters name if it is in author mapping
        commenter = combined_author_mapping.get(commenter, commenter)

        # New issue
        issue_to_commenters[issue_id].append(commenter)

    return issue_to_commenters


def generate_date_to_top_commenters(project_name, sws):
    """
    Generate a mapping from date to number of comment made until that date.

    Large change sets are not exluded because the comments made to the issues related
    to the large change sets still exist.

    Parameters
    ----------
    project_name (str):
        Name of the project

    sws (int):
        Sliding_window_size, in other words number of days to include the graph.

    Returns
    --------
    dict:
        Mapping from date to top commenters and their numbers of comments in the sliding
        window ending that date.
    """
    issue_to_commenters = generate_issue_to_commenters(project_name)
    data_manager = DataManager(get_dataset_path(project_name), sws)

    # Get initial change sets to add and remove
    change_sets_add = data_manager.get_initial_window()
    change_sets_remove = {}
    top_commenters = defaultdict(lambda: 0)

    date_to_top_commenters = {}
    while True:
        # Add change sets
        for change_set in change_sets_add:
            for issue_id in change_set.issues:
                for commenter in issue_to_commenters.get(issue_id, []):
                    top_commenters[commenter] += 1

        # Remove change sets
        for change_set in change_sets_remove:
            for issue_id in change_set.issues:
                for commenter in issue_to_commenters.get(issue_id, []):
                    top_commenters[commenter] -= 1
                    if top_commenters[commenter] <= 0:
                        del top_commenters[commenter]

        date = data_manager.get_last_included_date()
        date_to_top_commenters[date] = sort_dict(
            top_commenters, by_value=True, reverse=True
        )

        try:
            change_sets_add, change_sets_remove = data_manager.forward_one_day()
        except SlidingNotPossible:
            break

    return date_to_top_commenters


if __name__ == "__main__":
    # Lets extract top10 commenters into csv files to check manually.
    for project_name in project_list:
        for sws in sws_list:
            date_to_top_commenters = generate_date_to_top_commenters(project_name, sws)

            text = ""
            for date, counter in date_to_top_commenters.items():
                top10_commenters = highest_k(counter, 10)
                text += date_to_str(date) + "," + ",".join(top10_commenters) + "\n"

            with open(
                "data/{}_top_commenters.csv".format(project_name), "w", encoding="utf8"
            ) as f:
                f.write(text)
