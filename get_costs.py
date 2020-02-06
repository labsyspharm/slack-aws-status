import matplotlib
matplotlib.use('Agg')
import boto3
import pandas
import datetime
import matplotlib.pyplot as plt
from slackclient import SlackClient


def make_plot(start_date=None, end_date=None):
    if end_date is None:
        # Get today's date
        end_date = datetime.datetime.today()
    if start_date is None:
        # Get today - 7 days
        start_date = end_date - datetime.timedelta(7)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    client = boto3.client('ce')

    args = {'TimePeriod':
                {'Start': start_date_str,
                 'End': end_date_str},
            'Granularity': 'DAILY',
            'GroupBy': [
                {'Type': 'TAG', 'Key': 'project'}
                ],
            'Metrics': ['BlendedCost']}

    res = client.get_cost_and_usage(**args)
    data = {}
    for entry in res['ResultsByTime']:
        start = datetime.datetime.strptime(entry['TimePeriod']['Start'],
                                           '%Y-%m-%d').date()
        data[start] = {}
        for group in entry['Groups']:
            project_name = ''
            for key in group['Keys']:
                if key.startswith('project$'):
                    project_name = key[len('project$'):]
                    break
            if not project_name:
                project_name = 'NA'
            cost = group['Metrics']['BlendedCost']['Amount']
            data[start][project_name] = float(cost)

    df = pandas.DataFrame.from_dict(data)
    df = df.fillna(0)
    df.transpose().plot(rot=90, sort_columns=True)
    plt.subplots_adjust(left=0.09, right=0.97, top=0.97, bottom=0.21)
    fname = '%s.png' % end_date_str
    plt.ylabel('USD')
    plt.savefig(fname)
    return fname


def read_slack_token(fname=None):
    # Token can be found at https://api.slack.com/web#authentication
    if fname is None:
        fname = 'indrabot_slack_token'
    with open(fname, 'rt') as fh:
        token = fh.read().strip()
    return token


def send_message(channel, fname):
    token = read_slack_token()
    sc = SlackClient(token)
    sc.api_call('files.upload',
                channels=channel,
                filename=fname,
                filetype='png',
                file=open(fname, 'rb'),
                text='Cost report')


if __name__ == '__main__':
    #fname = make_plot(datetime.datetime(2019, 1, 1),
    #                  datetime.datetime(2019, 12, 31))
    fname = make_plot()
    send_message('C3V69UYAC', fname)