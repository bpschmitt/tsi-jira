from jira import JIRA
import logging
import json
import dateutil.parser, dateutil.tz
import time
from time import sleep
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Work in progress

class TSIevent:

    headers = {"Content-type": "application/json"}

    def __init__(self, tsiemail, tsiapikey, source, sender):
        self.email = tsiemail
        self.apikey = tsiapikey
        self.source = source
        self.sender = sender

    def send_event(self, url, event, headers):
        r = requests.post(url, data=event, headers=headers, auth=(self.email, self.apikey))
        return r.status_code

with open('param.json') as json_data:
    parms = json.load(json_data)

def connect_jira(log, jira_server, jira_user, jira_password):

    try:
        log.info("Connecting to JIRA: %s" % jira_server)
        jira_options = {'server' : jira_server }
        jira = JIRA(options=jira_options, basic_auth=(jira_user, jira_password))

        return jira

    except Exception as e:
        log.error("Failed to connect to JIRA: %s" % e)

        return None

def convert_timestamp(timestamp):
    ts = dateutil.parser.parse(timestamp)
    ts = int(time.mktime(ts.timetuple()))
    return ts

def parse_components(components):
    stuff = []
    for c in components:
        stuff.append(c['name'])

    return ', '.join(stuff)

def parse_labels(labels):
    return ', '.join(labels)

def convert_time(ts):

    if ts is None:
        return 0
    else:
        return int(ts)

def check_assignee(assignee):

    if assignee is None:
        return "Unassigned"
    else:
        return str(assignee['displayName'])

# Retrieve a single page and report the url and contents
def send_event(event, issue_id):
    #return event
    r = requests.post(parms['url'], data=json.dumps(event), headers=parms['headers'], auth=(parms['tsiemail'], parms['tsiapikey']))
    return "Issue ID: %s - Status: %s" % (issue_id,r.status_code)

#tsievent = TSIevent()
events = []
log = logging.getLogger(__name__)
jc = connect_jira(log, parms['server'], parms['username'], parms['password'])

projects = jc.projects()
# print(projects)

for p in projects:

    # Search for a single issue - testing purposes only
    issues = jc.search_issues("project='%s'" % p.name)

    # print(issues)
    for issue in issues:
        # print(issue.raw['fields'])
        # print("Project Name:%s - Issue Name:%s - Created:%s" % (p.name,issue,convert_timestamp(issue.raw['fields']['created'])))
        # print("Title:%s" % issue.raw['fields']['summary'])
        # print("Status:%s" % issue.raw['fields']['status']['name'])
        # print(parse_components(issue.raw['fields']['components']))

        event = {
            "source": parms["source"],
            "sender": parms["sender"],
            "fingerprintFields": ["@title", "app_id", "issue_id"],
            "title": issue.raw['fields']['summary'],
            "status": issue.raw['fields']['status']['name'],
            "createdAt": int(convert_timestamp(issue.raw['fields']['created'])),
            "eventClass": "Jira %s" % issue.raw['fields']['issuetype']['name'],
            "severity": issue.raw['fields']['issuetype']['name'],
            "properties": {
                "app_id": str(p.name).replace(" ", "_"),
                "assignee": check_assignee(issue.raw['fields']['assignee']),
                "components": parse_components(issue.raw['fields']['components']),
                "created_time": int(convert_timestamp(issue.raw['fields']['created'])),
                "creator": issue.raw['fields']['creator']['displayName'],
                "description": issue.raw['fields']['description'],
                "estimated_time": convert_time(issue.raw['fields']['timeestimate']),
                "fix_version": parse_components(issue.raw['fields']['fixVersions']),
                "issue_id": str(issue),
                "issue_type": issue.raw['fields']['issuetype']['name'],
                "labels": parse_labels(issue.raw['fields']['labels']),
                "logged_time": convert_time(issue.raw['fields']['timespent']),
                "priority": issue.raw['fields']['priority']['name'],
                "project": issue.raw['fields']['project']['name'],
                "project_key": issue.raw['fields']['project']['key'],
                "remaining_time": convert_time(issue.raw['fields']['timeestimate']) - convert_time(issue.raw['fields']['timespent']),
                "reporter": issue.raw['fields']['reporter']['displayName'],
                "updated_time": int(convert_timestamp(issue.raw['fields']['updated']))
            },
            "tags": [str(p.name).replace(" ", "_")]
        }

        #print(event)
        print("Adding event %s" % str(issue))
        events.append(event)

        #print("Issue: %s - Status: %s - Reason:%s" % (str(issue), r.status_code, r.reason))

print("Done building events...")
pool = ThreadPoolExecutor(10)
futures =[]
for i in range(5):
    print("Countdown: %s" % i)
    sleep(1)

print("Begin sending events...")
counter = 0
chunk = 0
for event in events:
    if counter < parms['chunksize']:
        #print(counter)
        futures.append(pool.submit(send_event,event,event['properties']['issue_id']))
        counter = counter + 1
    else:
        chunk = chunk + parms['chunksize']
        print("Chunk %s" % str(chunk))
        sleep(5)
        counter = 0

for x in as_completed(futures):
    print(x.result())


# # We can use a with statement to ensure threads are cleaned up promptly
# with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
#     # Start the load operations and mark each future with its URL
#     future_to_event = {executor.submit(send_event, j, 60): j}
#     for future in concurrent.futures.as_completed(future_to_event):
#         call = future_to_event[future]
#         try:
#             data = future.result()
#         except Exception as exc:
#             print('%r generated an exception: %s' % (call, exc))
#         else:
#             print('%r page is %d bytes' % (call, len(data)))
#
# ######
