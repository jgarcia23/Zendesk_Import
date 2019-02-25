import json
import csv
import time
import requests


# Author: Joseluis Garcia
# jgarcia23

# Zendesk_importer is to import the data from the 4 files contained in the Zip file into the
# zendesk instance. Create tickets, along with their associated users. The
# users will include both agent level users, who may be assigned to the tickets, and end-users who
# will have requested or commented on the tickets. Additionally, the end-users may be members of
# one or more organizations.


# Function creates a map for each organization with the organizations external_id as a key and generated id as a value
def get_org_map(session):
    # end-point
    url = 'https://z3nplatformdevjg.zendesk.com/api/v2/organizations.json '
    org_map = {}
    next_page = True

    while (next_page):
        r = session.get(url)
        response = r.json()

        for org in response['organizations']:
            org_map[org['external_id']] = org['id']
        if response['next_page'] == None:
            next_page = False
        else:
            url = response['next_page']
    return org_map


# Function creates a map for each user with their external_id as a key and generated id as a value
def get_user_map(session):
    url = 'https://z3nplatformdevjg.zendesk.com/api/v2/users.json '
    user_map = {}
    next_page = True

    while (next_page):  # keep looping until next_page equals null
        r = session.get(url)
        response = r.json()
        for user in response['users']:
            user_map[user['external_id']] = user['id']
        if response['next_page'] == None:
            next_page = False
        else:
            url = response['next_page']
    return user_map


# create dict with "parent_ticket_id" as key and value is the comment body then return;
def get_comments(file, user_map):
    comments_map = {}  # dict to return full of comments

    for i in range(1, len(file)):  # iterate through all file comments

        author_id = validate(file[i][1])
        html_body = validate(file[i][2])
        public = validate(file[i][3])
        created_at = validate(file[i][4])
        parent_ticket_id = validate(file[i][5])

        if author_id in user_map:
            author_id = user_map[author_id]
        else:
            author_id = '376281270772'

        data = {
            "author_id": author_id,
            "html_body": html_body,
            "public": public,
            "created_at": created_at,
            "parent_ticket_id": parent_ticket_id
        }
        if parent_ticket_id not in comments_map:
            comments_map[parent_ticket_id] = [data]
        else:
            comments_map[parent_ticket_id].append(data)

    return comments_map


# Function translates statuses from Legacy Status to Zendesk Status
def ticket_status_filter(status):
    if status == "assigned":
        status = "Open"
    elif status == "waiting":
        status = "Pending"
    elif status == "external" or status == "engineering":
        status = "On Hold"
    elif status == "resolved":
        status = "Solved"
    elif status == "done" or status == "retracted":
        status = "Closed"
    return status


# Function checks for special cases were the submitter,requester, or assignee do not exist
# then use a Generic Agent user (ID: 376281270772)
def check_user_exist(submitter, requester, assignee, user_map, data):
    data = data

    if submitter not in user_map:
        submitter = '376281270772'
        data.update({"submitter_id": submitter})
    else:
        data.update({"submitter_id": user_map[submitter]})

    if requester not in user_map:
        requester = '376281270772'
        data.update({"requester_id": requester})
    else:
        data.update({"requester_id": user_map[requester]})

    if assignee not in user_map:
        assignee = '376281270772'
        data.update({"assignee_id": assignee})
    else:
        data.update({"assignee_id": user_map[assignee]})
    return data


# Function validates that the field exist otherwise replace with None
def validate(field):
    temp = len(field)
    if temp == 0:
        return None
    return field


# Function cleans out the data by replacing all error regex and returns a clean string
def validate_array(str):
    for r in (("'", '"'), ("(", ''), (")", ''), ("_", ''), ("-", '')):
        str = str.replace(*r)
    return str.strip()


#  Function checks if users is linked to multi organizations
def validate_organization(org_value, org_map, org_array):
    if org_value[0] == '[':
        external_org_ids = json.loads(validate_array(org_value))

        # convert each external org id to zendesk ids
        org_array = list(map(lambda org_id: org_map[org_id], external_org_ids))

        return None, org_array

    else:
        return org_map[org_value], org_array


# Check the job statuses of each payload delivered
def check_job_statuses(status_list, session):
    completed = False

    while not completed:  # loop until all jobs are done
        success_count = 0
        response = send_job_statuses_request(status_list, session)
        for job_status in response['job_statuses']:
            status = job_status['status']
            if status == "queued" or status == "working":
                print('In progress...')
                print('In progress`: ', job_status)
                time.sleep(10)
                break
            elif status == "failed" or status == "killed":
                print('Failed')
                exit()
            elif status == 'completed':
                success_count += 1
                total_count = response['count']
                print('Success count: ', success_count, '/', total_count)
                if success_count == total_count:
                    completed = True
                    print('Successfully imported a batch')

# Get request from URL endpoint and handles 429 rate limited error
def send_job_statuses_request(status_list, session, retry_attempts=5):
    print(status_list)
    status_id_list = list(map(lambda status: status['job_status']['id'], status_list))

    # TODO validate that len status_id_list <= 100, break up into multi request if not
    # creating a string of id to show i.g  ids= 1,2,3
    url_paste = ",".join(str(x) for x in status_id_list)
    url = 'https://z3nplatformdevjg.zendesk.com/api/v2/job_statuses/show_many.json?ids=' + url_paste

    try:
        retry_attempts -= 1 # Decrement
        r = session.get(url)
        r.raise_for_status()
        job_statuses = r.json()
        return job_statuses

    except requests.HTTPError as err:
        if r.status_code == 429:  # rate limited error code
            print("Rate is limited. Waiting to retry...")
            time.sleep(int(r.headers['retry-after']))
            if retry_attempts > 0:
                send_job_statuses_request(status_list, session, retry_attempts) # recurse until job is done 5 tries max
            else:
                print("Retry Limit reached")
        print(err)

# sends payloads
def send_payloads(URL, payloads, session, type):
    status_list = []
    # iterates through the payloads and post one payload at a time to a Zendesk Instance
    for payload in payloads:
        send_create_many_request(URL, payload, session, status_list, type)
    # Poll job status endpoint until all jobs complete or any one job fails
    check_job_statuses(status_list, session)

#  Post request to URL endpoint and handle 400 < errors
def send_create_many_request(URL, payload, session, status_list, type, retry_attempts=5):
    try:
        retry_attempts -= 1
        r = session.post(URL, data=payload)
        r.raise_for_status()
        job_status = r.json()
        status_list.append(job_status)
    except requests.HTTPError as err:
        if r.status_code == 429:  # rate limited error code
            print("Rate is limited. Waiting to retry...")
            time.sleep(int(r.headers['retry-after']))
            if retry_attempts > 0:
                send_create_many_request(URL, payload, session, status_list, type, retry_attempts) # recurse
            else:
                print("Retry Limit reached")
        print(err)
        print_external_ids(payload, type)

# get external ID's from payload being sent
def print_external_ids(payload, type):
    failed_items = json.loads(payload)[type]
    # Ternary Operator (a if condition else b)
    failed_ids = failed_items if type == 'organization_memberships' else list(map(lambda resource: resource['external_id'], failed_items))
    print("Failed to import this batch of ", type, ": ", failed_ids)


def import_tickets(tickets, session, user_map, comments_map):
    URL = 'https://z3nplatformdevjg.zendesk.com/api/v2/imports/tickets/create_many.json'  # api-endpoint (502 error)
    payloads = []  # array of payloads to be sent
    tickets_dict = {"tickets": []}  # tickets dictionary

    # iterate through all tickets
    for i in range(1, len(tickets)):
        comments = []
        #  tagging my name
        try:
            tags = json.loads(validate_array(tickets[i][17]))
            tags.append('joseluis')
        except IndexError:
            continue
        # initialize all variable fields for the data body
        external_id = validate(tickets[i][0])
        created_at = validate(tickets[i][2])
        subject = validate(tickets[i][3])
        description = validate(tickets[i][4])
        status = validate(tickets[i][5])
        updated_at = validate(tickets[i][8])
        due_at = validate(tickets[i][9])
        about = validate(tickets[i][10])
        business_name = validate(tickets[i][11])
        dept = validate(tickets[i][12]),
        emp_id = validate(tickets[i][13])
        product_information = validate(tickets[i][14])
        date = validate(tickets[i][15])
        subscription = validate(tickets[i][16])
        submitter = tickets[i][6]
        requester = tickets[i][7]
        assignee = tickets[i][1]
        comments.extend(comments_map[external_id]) # append comments in json format

        data = {
            "external_id": external_id,
            "created_at": created_at,
            "subject": subject,
            "description": description,
            "status": ticket_status_filter(status),
            "updated_at": updated_at,
            "due_at": due_at,
            "tags": tags,
            "comments": comments,
            "custom_fields": {
                "about": about,
                "business_name": business_name,
                "dept": dept,
                "emp_id": emp_id,
                'product_information': product_information,
                'start date': date,
                'subscription': subscription
            }

        }
        # Check for special cases were the submitter,requester, or assignee do not exist
        # then use a Generic Agent user (ID: 376281270772)
        data = check_user_exist(submitter, requester, assignee, user_map, data)

        tickets_dict["tickets"].append(data)  # add data to tickets dict

        # check if dict reaches the limit of 100 if-so dump in payloads
        if len(tickets_dict["tickets"]) == 50:
            payloads.append(json.dumps(tickets_dict))
            tickets_dict = {"tickets": []}  # reset dict

    # check if any data is in the dictionary since it does not always reach 100
    if tickets_dict["tickets"]:
        payloads.append(json.dumps(tickets_dict))

    send_payloads(URL, payloads, session, 'tickets')


# Function creates multi organization memberships for users with more than one membership
def import_org_memberships(session, org_memberships, user_map):
    URL = "https://z3nplatformdevjg.zendesk.com/api/v2/organization_memberships/create_many.json"
    payload = []
    org_dict = {"organization_memberships": []}

    for member in org_memberships:

        # initialize variable fields for the data body
        user_id = user_map[member]
        membership_array = org_memberships[member]

        for org in membership_array:
            org_id = org
            data = {
                "user_id": int(user_id),
                "organization_id": int(org_id)
            }
            # check if it is the first organization set default as true else false
            if org == membership_array[0]:
                data.update({"default": True})
            else:
                data.update({"default": False})

            org_dict["organization_memberships"].append(data)  # add data to dictionary

            # check if org_dict reaches the limit of 100 if-so dump in payload
            if len(org_dict["organization_memberships"]) == 100:
                payload.append(json.dumps(org_dict))
                org_dict = {"organization_memberships": []}  # reset dict

    if org_dict["organization_memberships"]:  # check if any extra data is in the dic
        payload.append(json.dumps(org_dict))

    send_payloads(URL, payload, session, 'organization_memberships')  # send payloads


# Function creates organizations into the zendesk instance
def import_organizations(organizations, session):
    URL = "https://z3nplatformdevjg.zendesk.com/api/v2/organizations/create_many.json"
    payloads = []  # payloads to be sent
    org_dict = {"organizations": []}
    status_list = []

    # iterate through all organizations
    for i in range(1, len(organizations)):

        #  tagging my name
        tags = json.loads(validate_array(organizations[i][6]))
        tags.append('joseluis')

        domain_names = json.loads(validate_array(organizations[i][2]))

        # initialize all variable fields for the body
        id = validate(organizations[i][0])
        name = validate(organizations[i][1])
        domain_names = validate(domain_names)
        details = validate(organizations[i][3])
        notes = validate(organizations[i][4])
        merchant_id = validate(organizations[i][5])

        # formatting into proper format for api
        data = {
            "external_id": int(id),
            "name": name,
            "domain_names": domain_names,
            "details": details,
            "notes": notes,
            "tags": tags,
            "organization_fields": {
                "merchant_id'": merchant_id
            }
        }

        org_dict["organizations"].append(data)  # add data to dict

        # create payloads with max 100 items each
        if len(org_dict["organizations"]) == 100:
            payloads.append(json.dumps(org_dict))
            org_dict = {"organizations": []}

    # check if there are any items the last payload < 100
    if org_dict["organizations"]:
        payloads.append(json.dumps(org_dict))

    # iterate through the payloads list and post one payload at a time to Zendesk Support
    send_payloads(URL, payloads, session, "organizations")


# Function creates both end-users and agent/admin users into the zendesk instance
def import_users(users, session, org_map, org_memberships):
    # api-endpoints
    end_users_url_endpoint = "https://z3nplatformdevjg.zendesk.com/api/v2/users/create_many.json"
    agent_url_endpoint = "https://z3nplatformdevjg.zendesk.com/api/v2/users/create_or_update_many.json"

    end_user_payload = []
    agents_payload = []
    emails_map = {}  # contains emails with their frequency occurance

    end_users_dict = {"users": []}  # map of end users
    agent_users_dict = {"users": []}  # map of end users

    # creating dict of emails with its frequency in the data
    for user in users:
        user_email = user[2]
        if (user_email not in emails_map):
            emails_map[user[2]] = 1
        else:
            emails_map[user[2]] += 1

    # iterate through all users
    for i in range(1, len(users)):

        org_array = []

        if (emails_map[users[i][2]]) <= 1 or (users[i][2] == ''):

            #  tagging my name
            tags = ['joseluis']
            try:
                tags = json.loads(validate_array(users[i][11]))
                tags.append("joseluis")

            except IndexError:
                continue

            # initialize all variable fields for the data body
            id = validate(users[i][0])
            name = validate(users[i][1])
            email = validate(users[i][2])
            role = validate(users[i][4])
            active = validate(users[i][5])
            notes = validate(users[i][6])
            employee_id = validate(users[i][9])
            subscription = validate(users[i][8])
            promotion_code = validate(users[i][10])

            # validate that organization id is not an array otherwise return none
            # and append org array for membership
            organization_id, org_array = validate_organization(
                validate(users[i][3]), org_map, org_array)

            data = {

                "name": name,
                "email": email,
                "role": role,
                "active": active,
                "notes": notes,
                "tags": tags,
                "external_id": id,
                "user_fields": {
                    "employee_id": employee_id,
                    "subscription": subscription,
                    "promotion_code": promotion_code
                }
            }
            # associate single organizations with user un-creation
            if organization_id:
                data.update({"organization_id": int(organization_id)})
            else:
                org_memberships[id] = org_array  # associate all multiple org memberships with external user id until we can look up zendesk id

            # append to correct dictionary either end-users or agent and admin users
            if role == 'end-user':
                end_users_dict["users"].append(data)

                # check if dict reaches the limit of 100 if-so dump in payloads
                if len(end_users_dict["users"]) == 100:
                    end_user_payload.append(json.dumps(end_users_dict))
                    end_users_dict = {"users": []}  # reset dict

            elif role == 'agent' or role == 'admin':
                agent_users_dict["users"].append(data)

                if len(agent_users_dict["users"]) == 100:  # check if dict reaches the limit of 100 if-so dump in payloads
                    agents_payload.append(json.dumps(agent_users_dict))
                    agent_users_dict = {"users": []}  # reset dict

    # check if any end-users or agents are in dictionaries since they do not always reach 100
    if end_users_dict["users"]:
        end_user_payload.append(json.dumps(end_users_dict))
    if agent_users_dict["users"]:
        agents_payload.append(json.dumps(agent_users_dict))

    send_payloads(end_users_url_endpoint, end_user_payload, session, "users")  # send end-users payloads

    send_payloads(agent_url_endpoint, agents_payload,session, "users")


# This function reads the data from the csv file
def read_csv(file):
    csvReader = csv.reader(file)
    data = list(csvReader)
    return data


# Main function
def main():
    # Use session object to persist the api credentials across request.
    session = requests.Session()
    session.headers = {'Content-Type': 'application/json'}
    session.auth = 'username', 'password'

    # Opening csv files
    organizations_file = open('organizations.csv')
    comments_file = open('ticket_comments.csv')
    tickets_file = open('tickets.csv')
    users_file = open('users.csv')

    # Reading the data from the csv file
    organizations_data = read_csv(organizations_file)
    comments_data = read_csv(comments_file)
    tickets_data = read_csv(tickets_file)
    users_data = read_csv(users_file)

    import_organizations(organizations_data, session)  # Create organizations
    org_map = get_org_map(session)    # organization map with { external id: org_id }
    org_memberships = {}  # map of members with multi memberships

    import_users(users_data, session, org_map, org_memberships)  # Create users
    user_map = get_user_map(session)  # user map with {external id: user_id }

    import_org_memberships(session, org_memberships, user_map) # Create memberships for users with multi org's
    comments_map = get_comments(comments_data,user_map)  # get

    import_tickets(tickets_data, session, user_map, comments_map) # create tickets
    exit(0)

if __name__ == '__main__':
    main()
