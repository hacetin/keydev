"""
Includes dash components and callbacks to update them.
"""
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State
from datetime import datetime, timedelta, time
from read_helper import all_exps
import plotly.graph_objects as go

print("LOG:", "start")

exps = all_exps()
possible_experiment_names = list(exps.keys())
initial_exp = possible_experiment_names[0]
initial_chart_range = 30

print("LOG:", "completed data read")

app = dash.Dash(__name__)
app.title = "Key Developers"
server = app.server


### HELPER METHODS


def str_to_date(d_str: str):
    """
    Convert given date string to datetime.datetime object.
    """
    d = datetime.strptime(d_str.split("T")[0], "%Y-%m-%d")
    return clean_date(d)


def clean_date(d: datetime):
    """
    Remove parts other than year, month and day.
    """
    return datetime(year=d.year, month=d.month, day=d.day)


def date_format(d: datetime):
    """
    Convert given datetime.datetime object to date string.
    """
    return d.strftime("%d-%b-%Y")


def max_of_day(date):
    """
    Create a copy of the given date with the last hour, the last minute,
    the last second and the last milisecond of the day.
    """
    return datetime.combine(date, time.max)


def float_format(f):
    """
    If the given `f` is a number, format it with 5 decimals. Otherwise, return `f`
    without changing it.
    """
    try:
        return "{:.5f}".format(f)
    except:
        return f


def get_trigger_component(callback_context):
    """
    Extract component id from callbacl content
    """
    return callback_context.triggered[0]["prop_id"].split(".")[0]


### BANNER


def build_banner():
    """
    Create a div for the topmost part of the page (in other words banner).
    """
    return html.Div(
        id="banner",
        children=[
            html.Div(
                children=[
                    html.H3("Manager Dashboard"),
                    html.H5("Monitoring key developers"),
                ],
            ),
        ],
    )


### CONTROL PANEL


def experiment_picker_div():
    """
    Create a div which includes a dropdown menu to select an experiment.
    """
    return html.Div(
        id="experiment-picker",
        className="control-panel-box",
        children=[
            html.P("Experiment name"),
            dcc.Dropdown(
                id="experiment-name-dropdown",
                options=[
                    {"label": exp_name, "value": exp_name}
                    for exp_name in possible_experiment_names
                ],
                value=initial_exp,
                clearable=False,
            ),
        ],
    )


def traverse_one_day_div():
    """
    Create a div with backward and forward buttons.
    """
    return html.Div(
        id="traverse-buttons",
        className="control-panel-box",
        children=[
            html.P("Go backward or forward one day."),
            html.Div(
                style={"display": "flex", "flex-direction": "row"},
                children=[
                    html.Button("Backward", id="backward-button"),
                    html.Button("Forward", id="forward-button"),
                ],
            ),
        ],
    )


def traverse_date_content(today, min_date_allowed, max_date_allowed):
    """
    Create a div with a date picker.
    """
    return [
        html.P(
            "Select a specific date from {} to {}.".format(
                date_format(min_date_allowed), date_format(max_date_allowed)
            )
        ),
        dcc.DatePickerSingle(
            id="date-picker-single",
            date=today,
            min_date_allowed=min_date_allowed,
            max_date_allowed=max_date_allowed,
            display_format="DD-MMM-YYYY",
        ),
    ]


def traverse_date_div():
    """
    Create a div to wrap the date picker and set it.
    """
    return html.Div(
        id="traverse-date-div",
        className="control-panel-box",
        children=traverse_date_content(
            clean_date(exps[initial_exp]["dates"][-1]),
            clean_date(exps[initial_exp]["dates"][0]),
            clean_date(exps[initial_exp]["dates"][-1]),
        ),
    )


def control_panel():
    """
    Create a div to wrap the experiment picker, the backward button, the forward button
    and the date picker.
    """
    return html.Div(
        id="control-panel",
        children=[
            experiment_picker_div(),
            traverse_one_day_div(),
            traverse_date_div(),
        ],
    )


### SUMMARY PANEL


def developer_table(exp, today):
    """
    Create a table including list of developers and their scores in different categories.
    """
    if exp == None or today == None:
        return dash_table.DataTable(id="developer-table")

    return dash_table.DataTable(
        id="developer-table",
        columns=[
            {"name": i, "id": i}
            for i in ["Developer Name", "Jack", "Maven", "Connector"]
        ],
        data=[
            {
                "Developer Name": dev,
                "Jack": float_format(
                    exps[exp]["results"][today]["jacks"].get(dev, "-")
                ),
                "Maven": float_format(
                    exps[exp]["results"][today]["mavens"].get(dev, "-")
                ),
                "Connector": float_format(
                    exps[exp]["results"][today]["connectors"].get(dev, "-")
                ),
            }
            for dev in sorted(exps[exp]["results"][today]["developers"])
        ],
        # fixed_rows={'headers': True}, # There is a bug, headers slip
        style_cell={"width": "auto", "textAlign": "center"},
        style_table={"maxHeight": "30rem", "overflowY": "scroll"},
        style_cell_conditional=[
            {"if": {"column_id": "Developer Name"}, "textAlign": "left"}
        ],
        style_as_list_view=True,
    )


def hover_text(l):
    """
    Create a readable text to show when hovering the venn diagram.
    """
    # return "<br>".join(l)
    if not l:
        return ""

    l = list(l)
    text = l[0] + ", "
    for i in range(1, len(l)):
        if i % 3 == 0:
            text += "<br>"
        text += l[i] + ", "

    return text[:-2]


def info_panel(exp, today):
    """
    Create a simple text to share some information for selected date.
    """
    if not exp or not today:
        return html.P()

    num_devs = len(exps[exp]["results"][today]["developers"])
    num_files = exps[exp]["results"][today]["num_files"]
    num_reachable_files = exps[exp]["results"][today]["num_reachable_files"]
    per_reachable_files = 100 * num_reachable_files / num_files
    num_rare_files = exps[exp]["results"][today]["num_rare_files"]
    per_rare_files = 100 * num_rare_files / num_files
    balanced_or_hero = exps[exp]["results"][today].get("balanced_or_hero", "-")

    return html.P(
        id="info-p",
        children=[
            "{} developers ({} team), {} files, {} ({:.2f}%) reachable files and {} ({:.2f}%) rarely reachable files.".format(
                num_devs,
                balanced_or_hero,
                num_files,
                num_reachable_files,
                per_reachable_files,
                num_rare_files,
                per_rare_files,
            )
        ],
    )


def venn_diagram(exp, today):
    """
    Create a venn diagram showing all categories with their intersection combinations.
    """
    if exp == None or today == None:
        return html.Div()  # dcc.Graph(id="venn-diagram")

    fig = go.Figure()

    # Add circles
    fig.add_shape(
        type="circle", fillcolor="red", x0=0, y0=1, x1=1, y1=2, line_color="red"
    )
    fig.add_shape(
        type="circle", fillcolor="blue", x0=0.5, y0=1, x1=1.5, y1=2, line_color="blue"
    )
    fig.add_shape(
        type="circle",
        fillcolor="black",
        x0=0.25,
        y0=0.5,
        x1=1.25,
        y1=1.5,
    )

    # Create scatter trace of text labels
    jacks = set(exps[exp]["results"][today]["jacks"])
    mavens = set(exps[exp]["results"][today]["mavens"])
    connectors = set(exps[exp]["results"][today]["connectors"])

    jm = jacks.intersection(mavens).difference(connectors)
    mc = mavens.intersection(connectors).difference(jacks)
    jc = jacks.intersection(connectors).difference(mavens)
    jmc = set.intersection(jacks, mavens, connectors)
    j = jacks.difference(mavens).difference(connectors)
    m = mavens.difference(jacks).difference(connectors)
    c = connectors.difference(jacks).difference(mavens)

    fig.add_trace(
        go.Scatter(
            x=[0.5, 1, 0.75],
            y=[2.1, 2.1, 0.4],
            text=["Jacks", "Mavens", "Connectors"],
            mode="text",
            textfont=dict(
                color="black",
                size=18,
                family="Arail",
            ),
            hoverinfo="text",
        )
    )

    fig.add_trace(
        go.Scatter(
            mode="text",
            x=[0.25, 1.25, 0.75, 0.75, 0.45, 1.05, 0.75],
            y=[1.55, 1.55, 0.75, 1.65, 1.15, 1.15, 1.3],
            text=[
                len(j),
                len(m),
                len(c),
                len(jm),
                len(jc),
                len(mc),
                len(jmc),
            ],
            hovertext=[
                hover_text(j),
                hover_text(m),
                hover_text(c),
                hover_text(jm),
                hover_text(jc),
                hover_text(mc),
                hover_text(jmc),
            ],
            hoverinfo="text",
            textfont=dict(
                color="black",
                size=18,
                family="Arail",
            ),
        )
    )

    # Update axes properties
    fig.update_xaxes(
        showticklabels=False,
        showgrid=False,
        zeroline=False,
    )

    fig.update_yaxes(
        showticklabels=False,
        showgrid=False,
        zeroline=False,
    )

    fig.update_shapes(dict(opacity=0.3, xref="x", yref="y", layer="below"))
    fig.update_layout(
        plot_bgcolor="white", showlegend=False, margin=dict(l=20, r=20, t=10, b=0)
    )

    return html.Div(
        children=[info_panel(exp, today), dcc.Graph(id="venn-diagram", figure=fig)]
    )


def summary_panel(exp, today):
    """
    Create a div to wrap developer table and venn diagram
    """
    return html.Div(
        id="summary-panel",
        children=[
            developer_table(exp, today),
            venn_diagram(exp, today),
        ],
    )


### CHART PANEL


def score_table(exp, today, category):
    """
    Create a tbale with list of developers in given category and their scores in given
    category. Also, draw a red line after the last significant member of the category.
    """
    try:
        border_dev = exps[exp]["results"][today]["last_" + category[:-1]]
        if not border_dev:
            border_dev = ""
    except:
        border_dev = ""

    print("LOG:", "last_" + category[:-1], border_dev)

    return dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in ["Developer Name", "Score"]],
        data=[
            {
                "Developer Name": dev,
                "Score": float_format(score),
            }
            for dev, score in exps[exp]["results"][today][category].items()
        ],
        # fixed_rows={'headers': True}, # There is a bug, headers slip
        style_cell={"width": "auto", "textAlign": "left"},
        style_data_conditional=[
            {
                "if": {"filter_query": '{Developer Name} eq "' + border_dev + '"'},
                "borderBottom": "3px solid red",
            }
        ],
        style_table={
            "maxHeight": "30rem",
            "overflowY": "scroll",
        },
        style_as_list_view=True,
    )


def score_table_div(exp, today, category):
    """
    Create a div to wrap score table.
    """
    if exp == None or today == None:
        return html.Div(className="score-table-div")

    return html.Div(
        className="score-table-div", children=score_table(exp, today, category)
    )


def score_chart(exp, today, chart_range, category, top_dev):
    """
    Create a graph (chart or plot) shoing the scores of the developers over time.
    """
    if exp == None or today == None:
        return dcc.Graph()

    today_index = exps[exp]["dates"].index(today)
    start_index = max(0, today_index - chart_range + 1)

    developers = list(exps[exp]["results"][today][category])
    if top_dev != None:
        # Assume that developers are sorted according to their scores
        developers = developers[:top_dev]

    data = []
    for dev in ["AVERAGE"] + sorted(developers):
        y = exps[exp]["score_history"][category][dev][start_index : today_index + 1]

        if not any(y):
            continue

        x = [
            date_format(date)
            for date in exps[exp]["dates"][start_index : today_index + 1]
        ]
        data.append({"x": x, "y": y, "name": dev})

    return dcc.Graph(
        figure={
            "data": data,
            "layout": dict(
                xaxis={"title": "Date"},
                yaxis={"title": "Score", "range": (0, 1)},
                margin=dict(l=50, r=50, t=30, b=140),
            ),
        }
    )


def chart_options(category):
    """
    Create input fields to get chart range and number of top developers to show.
    """
    return html.Div(
        className="chart-options-div",
        style={"display": "flex", "flex-direction": "row"},
        children=[
            html.P("Chart range (1 - \u221e):"),
            dcc.Input(
                id="{}-chart-range-input".format(category),
                value=30,
                type="number",
                debounce=True,
                min=1,
            ),
            html.P("Top developers (1 - \u221e):"),
            dcc.Input(
                id="{}-top-dev-input".format(category),
                type="number",
                debounce=True,
                min=1,
            ),
        ],
    )


def score_chart_div(exp, today, chart_range, category):
    """
    Create a div to wrap chart options and chart.
    """
    return html.Div(
        className="score-chart-div",
        children=[
            chart_options(category),
            html.Div(
                id="{}-chart-div".format(category),
                children=score_chart(exp, today, chart_range, category, None),
            ),
        ],
    )


def score_panel(exp, today, chart_range, category):
    """
    Create a div to wrap score table div and corresponding chart.
    """
    return html.Div(
        className="score-panel",
        children=[
            score_table_div(exp, today, category),
            score_chart_div(exp, today, chart_range, category),
        ],
    )


def chart_panel_content(exp, today, chart_range):
    """
    Create the summary panel and score pannels for categories.
    """
    return [
        html.H3("Summary"),
        summary_panel(exp, today),
        html.H3("Jacks"),
        score_panel(exp, today, chart_range, "jacks"),
        html.H3("Mavens"),
        score_panel(exp, today, chart_range, "mavens"),
        html.H3("Connectors"),
        score_panel(exp, today, chart_range, "connectors"),
        html.H3("Replacements"),
        replacement_panel(exp, today),
    ]


def replacement_panel(exp, today):
    """
    Create a table for recommended replacements if any developer left the project today.
    """
    if not exp or not today:
        return

    replacements = exps[exp]["results"][today]["replacements"]
    if not replacements:
        return html.P("No one left today.")

    rows = []
    for leaving_dev, recom_dev_to_score in replacements.items():
        row = {"Leaving Developer": leaving_dev}
        recom_devs = list(recom_dev_to_score)
        for i in range(3):
            row["{}. Replacement".format(i + 1)] = "{} ({})".format(
                recom_devs[i], float_format(recom_dev_to_score[recom_devs[i]])
            )
        rows.append(row)

    return dash_table.DataTable(
        id="replacement-table",
        columns=[{"name": i, "id": i} for i in list(rows[0])],
        data=rows,
        style_cell={"width": "auto", "textAlign": "left"},
        style_table={"maxHeight": "30rem", "overflowY": "scroll"},
        style_as_list_view=True,
    )


def chart_panel():
    """
    Create a div to wrap chart contents.
    """
    return html.Div(
        id="chart-panel",
        children=chart_panel_content(None, None, initial_chart_range),
    )


### SET APP LAYOUT

app.layout = html.Div(
    children=[
        build_banner(),
        html.Div(
            id="app-container",
            children=[
                control_panel(),
                chart_panel(),
            ],
        ),
    ],
)


@app.callback(
    Output(component_id="traverse-date-div", component_property="children"),
    [
        Input(component_id="experiment-name-dropdown", component_property="value"),
        Input(component_id="backward-button", component_property="n_clicks"),
        Input(component_id="forward-button", component_property="n_clicks"),
    ],
    [
        State(component_id="date-picker-single", component_property="date"),
        State(component_id="date-picker-single", component_property="min_date_allowed"),
        State(component_id="date-picker-single", component_property="max_date_allowed"),
    ],
)
def update_traverse_date_div(
    exp,
    n_clicks_backward,
    n_clicks_forward,
    today,
    min_date_allowed,
    max_date_allowed,
):
    """
    Callback to update date picker. Triggered when "an experiment is selected" or
    "forward and backward buttons are clicked" or "a date is picked".
    """
    triggered_comp = get_trigger_component(dash.callback_context)
    print(
        "LOG: callback - update_traverse_date_div -> triggered by: {}, exp: {}".format(
            triggered_comp, exp
        )
    )

    today = str_to_date(today)
    min_date_allowed = str_to_date(min_date_allowed)
    max_date_allowed = str_to_date(max_date_allowed)

    if triggered_comp == "backward-button":
        new_date = today - timedelta(days=1)
        if new_date >= min_date_allowed:
            today = new_date
    elif triggered_comp == "forward-button":
        new_date = today + timedelta(days=1)
        if new_date <= max_date_allowed:
            today = new_date
    elif triggered_comp == "experiment-name-dropdown":
        min_date_allowed = clean_date(exps[exp]["dates"][0])
        max_date_allowed = clean_date(exps[exp]["dates"][-1])
        today = max_date_allowed

    return traverse_date_content(today, min_date_allowed, max_date_allowed)


@app.callback(
    Output(component_id="chart-panel", component_property="children"),
    [Input(component_id="date-picker-single", component_property="date")],
    [State(component_id="experiment-name-dropdown", component_property="value")],
)
def update_chart_panel(today, exp):
    """
    Callback to update all scores, venn diagram and chart. Triggered when "a date is
    picked". Implicitly triggered when "an experiment is selected" or "forward and
    backward buttons are clicked" because a date will be picked automatically when
    these happens.
    """
    comp_id = get_trigger_component(dash.callback_context)
    print(
        "LOG: callback - update_chart_panel -> triggered by: {}, today: {}".format(
            comp_id, today
        )
    )
    if today == None:
        return

    today = str_to_date(today)
    today = max_of_day(today)

    return chart_panel_content(exp, today, initial_chart_range)


@app.callback(
    Output(component_id="jacks-chart-div", component_property="children"),
    [
        Input(component_id="jacks-chart-range-input", component_property="value"),
        Input(component_id="jacks-top-dev-input", component_property="value"),
    ],
    [
        State(component_id="date-picker-single", component_property="date"),
        State(component_id="experiment-name-dropdown", component_property="value"),
    ],
)
def update_jacks_chart(chart_range, top_dev, today, exp):
    """
    Callback to update the jack chart. Triggered when "chart range is changed" or
    "number of top developers is changed".
    """
    comp_id = get_trigger_component(dash.callback_context)
    print("LOG: callback - update_jacks_chart -> ", end="")
    print(
        "triggered by: {}, chart_range: {}, top_dev: {}, today: {}".format(
            comp_id, chart_range, top_dev, today
        )
    )
    if today == None or exp == None:
        return

    if chart_range == None:
        chart_range = 1

    today = str_to_date(today)
    today = max_of_day(today)

    return score_chart(exp, today, chart_range, "jacks", top_dev)


@app.callback(
    Output(component_id="mavens-chart-div", component_property="children"),
    [
        Input(component_id="mavens-chart-range-input", component_property="value"),
        Input(component_id="mavens-top-dev-input", component_property="value"),
    ],
    [
        State(component_id="date-picker-single", component_property="date"),
        State(component_id="experiment-name-dropdown", component_property="value"),
    ],
)
def update_mavens_chart(chart_range, top_dev, today, exp):
    """
    Callback to update the maven chart. Triggered when "chart range is changed" or
    "number of top developers is changed".
    """
    comp_id = get_trigger_component(dash.callback_context)
    print("LOG: callback - update_mavens_chart -> ", end="")
    print(
        "triggered by: {}, chart_range: {}, today: {}".format(
            comp_id, chart_range, today
        )
    )
    if today == None or exp == None:
        return

    if chart_range == None:
        chart_range = 1

    today = str_to_date(today)
    today = max_of_day(today)

    return score_chart(exp, today, chart_range, "mavens", top_dev)


@app.callback(
    Output(component_id="connectors-chart-div", component_property="children"),
    [
        Input(component_id="connectors-chart-range-input", component_property="value"),
        Input(component_id="connectors-top-dev-input", component_property="value"),
    ],
    [
        State(component_id="date-picker-single", component_property="date"),
        State(component_id="experiment-name-dropdown", component_property="value"),
    ],
)
def update_connectors_chart(chart_range, top_dev, today, exp):
    """
    Callback to update the connector chart. Triggered when "chart range is changed" or
    "number of top developers is changed".
    """
    comp_id = get_trigger_component(dash.callback_context)
    print("LOG: callback - update_connectors_chart -> ", end="")
    print(
        "triggered by: {}, chart_range: {}, today: {}".format(
            comp_id, chart_range, today
        )
    )

    if today == None or exp == None:
        return

    if chart_range == None:
        chart_range = 1

    today = str_to_date(today)
    today = max_of_day(today)

    return score_chart(exp, today, chart_range, "connectors", top_dev)


if __name__ == "__main__":
    app.run_server(debug=True)
