import os
import io
import boto3
import pandas
import matplotlib
import matplotlib.pyplot as plt
import slack_sdk


# Number of days in rolling window for median calculation.
WINDOW_LENGTH = 30

cmap = matplotlib.colors.ListedColormap(matplotlib.colormaps['turbo_r'].colors[30:-30])

def make_plot(start_date=None, end_date=None):
    if end_date is None:
        # Get today's date
        end_date = pandas.Timestamp.today().floor('d')
    if start_date is None:
        # Get today - 7 days
        start_date = end_date - pandas.Timedelta(days=7)
    days_to_plot = (end_date - start_date).days
    data_start_date = start_date - pandas.Timedelta(days=WINDOW_LENGTH)

    client = boto3.client('ce')

    args = {
        'TimePeriod': {
            'Start': data_start_date.strftime('%Y-%m-%d'),
            'End': end_date.strftime('%Y-%m-%d'),
        },
        'Granularity': 'DAILY',
        'GroupBy': [
            {
                'Type': 'TAG',
                'Key': 'project',
            },
        ],
        'Metrics': ['BlendedCost'],
    }

    res = client.get_cost_and_usage(**args)

    df = pandas.DataFrame(res['ResultsByTime'])
    df = df.set_index(pandas.to_datetime(df['TimePeriod'].apply(pandas.Series)['Start'])).rename_axis('Date')
    df = df['Groups'].explode().apply(pandas.Series).explode('Keys')
    df = df[df['Keys'].str.startswith('project$')]
    df['Project'] = df['Keys'].str.replace('project$', '').replace('', '(untagged)')
    df['Cost'] = df['Metrics'].map(lambda x: x['BlendedCost']['Amount']).astype(float)
    df = df[['Project', 'Cost']]
    df = df.fillna(0)
    df = df.reset_index().pivot(index='Project', columns='Date', values='Cost')

    largest = df.loc[:, start_date:].max(axis='columns').sort_values(ascending=False)
    show = largest > largest.sum() * 0.01
    dfs = df.reindex(show.index).loc[show].transpose()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10,4))

    dfp1 = dfs.loc[start_date:].copy()
    dfp1['Date'] = dfp1.index.date
    dfp1.plot.bar(x='Date', ax=ax1, stacked=True, cmap=cmap)
    ax1.set_ylabel('Cost (USD)')

    dfp2 = (dfs / dfs.rolling(30).median()).loc[start_date:]
    dfp2['Date'] = dfp2.index.date
    dfp2.plot(x='Date', ax=ax2, rot=90, cmap=cmap, legend=False)
    ax2.set_ylabel('\n\nFold change over 30-day rolling median')

    ax1.legend(loc='center right', bbox_to_anchor=(-0.3, 0.5), reverse=True)
    fig.tight_layout(pad=1.08, h_pad=0)

    return fig


def save_plot():
    fig = make_plot()
    bio = io.BytesIO()
    fig.savefig(bio)
    bio.seek(0)
    return bio


def send_message(channel, upload_file, filename):
    token = os.environ["SLACK_APP_TOKEN"]
    client = slack_sdk.WebClient(token=token)
    client.files_upload_v2(
        file=upload_file,
        filename=filename,
        channel=channel,
    )


if __name__ == '__main__':
    #fname = make_plot(datetime.datetime(2019, 1, 1),
    #                  datetime.datetime(2019, 12, 31))
    upload_file = save_plot()
    title = pandas.Timestamp.today().strftime('usage_%Y%m%d.png')
    send_message('C3V69UYAC', upload_file, title)
