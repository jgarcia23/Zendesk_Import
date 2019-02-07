import json, csv
import requests


def create_tickets(tickets, session, user_map, org_map,comments):
    URL = 'https://z3nplatformdevjg.zendesk.com/api/v2/imports/tickets/create_many.json'
    payloads = []
    tickets_dict = {"tickets": []}

    for i in range(1, len(tickets)):
        #  tagging my name
        tags = json.loads(validate_array(tickets[i][17]))
        tags.append('joseluis')


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

        comments = []

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

        submitter = tickets[i][6]
        requester = tickets[i][7]
        assinee = tickets[i][1]

        if submitter not in user_map:
            submitter = '376281270772'
            data.update({"submitter_id":submitter})
        else:
            data.update({"submitter_id":user_map[submitter]})

        if requester not in user_map:
            requester = '376281270772'
            data.update({"requester_id": requester})
        else:
            data.update({"requester_id": user_map[requester]})

        if assinee not in user_map:
            assinee = '376281270772'
            data.update({"assignee_id": assinee})
        else:
            data.update({"assignee_id": user_map[assinee]})


        tickets_dict["tickets"].append(data)
        if len(tickets_dict["tickets"]) == 100:
            payloads.append(json.dumps(tickets_dict))
            tickets_dict = {"tickets": []}
    if tickets_dict["tickets"]:
        payloads.append(json.dumps(tickets_dict))

    for load in payloads:
        print(load)
        r = session.post(URL, data=load)
        result = r.json()
        print(result)



def ticket_status_filter(status):

    if status == "assigned":
        status = "Open"
    elif status ==  "waiting":
        status = "Pending"
    elif status == "external" or status == "engineering":
        status = "On Hold"
    elif status == "resolved":
        status = "Solved"
    elif status == "done" or status == "retracted":
        status = "Closed"

    return

def get_comments(file):

    payloads = []
    comments = []

    for i in range(1, len(file)):


        author_id = validate(file[i][1])
        html_body = validate(file[i][2])
        public = validate(file[i][3])
        created_at= validate(file[i][4])
        parent_ticket_id = validate(file[i][5])

        data = {
            "author_id": author_id,
            "html_body": html_body,
            "public": public,
            "created_at": created_at,
            "parent_ticket_id": parent_ticket_id,
        }

        comments.append(data)
    return comments

def get_user_map(session):
    url = 'https://z3nplatformdevjg.zendesk.com/api/v2/users.json '
    user_map = {}
    next_page = True

    while (next_page):
        r = session.get(url)
        response = r.json()

        for user in response['users']:
            user_map[user['external_id']] = user['id']

        if response['next_page'] == None:
            next_page = False
        else:
            url = response['next_page']
    return user_map


def create_org_memberships(session, org_memberships, user_map, org_map):
    URL = "https://z3nplatformdevjg.zendesk.com/api/v2/organization_memberships/create_many.json"
    payload = []
    org_dict = {"organization_memberships": []}

    for member in org_memberships:

        user_id = user_map[member]
        membership_array = org_memberships[member]

        for org in membership_array:
            org_id = org
            # print(org_id)
            data = {
                "user_id": int(user_id),
                "organization_id": int(org_id)

            }

            if org == membership_array[0]:
                data.update({"default": True})
            else:
                data.update({"default": False})

            org_dict["organization_memberships"].append(data)

            if len(org_dict["organization_memberships"]) == 100:
                payload.append(json.dumps(org_dict))
                org_dict = {"organization_memberships": []}

    if org_dict["organization_memberships"]:
        payload.append(json.dumps(org_dict))
    # print(payload)
    # for load in payload:
    #     r = session.post(URL, data=load)
    #     result = r.json()
    #     print(result)


def get_org_map(session):
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


def job_statuses(status_list, session):
    status_id_list = []
    for line in status_list:
        id = line['job_status']['id']  # get the id
        status_id_list.append(id)

    url_paste = ",".join(str(x) for x in status_id_list)
    url = 'https://z3nplatformdevjg.zendesk.com/api/v2/job_statuses/show_many.json?ids=' + url_paste
    r = session.get(url)
    response = r.json()
    print(response)
    # completed = False


# while not completed:
#
#     for status in response['job_statuses']:
#         status = status['status']
#         if status == "queued" or status == "working":
#             success_count = 0
#             print('In progress...')
#             time.sleep(10)
#             continue
#         elif status == "failed" or status == "killed":
#             print('Failed')
#             time.sleep(5)
#             exit()
#         elif status == 'completed':
#             success_count += 1
#             if success_count == response['count']:
#                 completed = True
#                 print('Successfully imported a batch of organizations')
#                 break
#             continue    # Continue checking job statuses if not all are completed
#     response = r.json()

def validate(field):
    temp = len(field)
    if temp == 0:
        return None
    return field


def validate_array(str):
    for r in (("'", '"'), ("(", ''), (")", ''), ("_", ''), ("-", '')):
        str = str.replace(*r)
    return str.strip()


def validate_organization(org_value, org_map, org_array):
    if org_value[0] == '[':
        external_org_ids = json.loads(validate_array(org_value))
        # convert each external org id to zendesk ids
        org_array = list(map(lambda org_id: org_map[org_id], external_org_ids))

        return None, org_array

    else:
        return org_map[org_value], org_array


def create_organizations(organizations, session, org_map):
    URL = "https://z3nplatformdevjg.zendesk.com/api/v2/organizations/create_many.json "
    payloads = []
    org_dict = {"organizations": []}
    status_list = []

    try:
        for i in range(1, len(organizations)):

            #  tagging my name
            tags = json.loads(validate_array(organizations[i][6]))
            tags.append('joseluis')

            domain_names = json.loads(validate_array(organizations[i][2]))

            id = validate(organizations[i][0])
            name = validate(organizations[i][1])
            domain_names = validate(domain_names)
            details = validate(organizations[i][3])
            notes = validate(organizations[i][4])
            merchant_id = validate(organizations[i][5])

            data = {
                "external_id": int(id),
                "name": name,
                "domain_names": domain_names,
                "details": details,
                "notes": notes,
                "tags": tags,
                "organization_fields": {
                    "merchant_id'": merchant_id,
                }
            }

            org_dict["organizations"].append(data)

            if len(org_dict["organizations"]) == 100:
                payloads.append(json.dumps(org_dict))
                org_dict = {"organizations": []}

    except IndexError:
        pass

    except ValueError:
        pass

    if org_dict["organizations"]:
        payloads.append(json.dumps(org_dict))
    # iterate through the payloads list and post one payload at a time to Zendesk Support
    # for payload in payloads:
    #     r = session.post(URL, data=payload)
    #     pastebin_url = r.json()
    #     print(pastebin_url)
        # status_list.append(pastebin_url)
    #
    # job_statuses(status_list, session)


def create_users(users, session, org_map, org_memberships):
    # api-endpoint
    URL = "https://z3nplatformdevjg.zendesk.com/api/v2/users/create_many.json"

    end_user_payload = []
    agents_payload = []

    emails_map = {}  # contains emails with their frequency occurance

    end_users_dict = {"users": []}  # map of end users
    agent_users_dict = {"users": []}  # map of end users
    count = 0

    # creating dict of emails with its frequency in the data
    for user in users:
        user_email = user[2]
        if (user_email not in emails_map):
            emails_map[user[2]] = 1
        else:
            emails_map[user[2]] += 1

    for i in range(1, len(users)):

        org_array = []
        if (emails_map[users[i][2]]) <= 1 or (users[i][2] == ''):

            #  tagging my name
            tags = ['joseluis']
            try:
                tags = json.loads(validate_array(users[i][11]))
                tags.append("joseluis")
            # count1 += 1
            except IndexError:
                continue

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
            organization_id, org_array = validate_organization(validate(users[i][3]), org_map, org_array)

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
                org_memberships[
                    id] = org_array  # associate all multiple org memberships with external user id until we can look up zendesk id

            # append to correct dictionary either end-users or agent and admin users
            if role == 'end-user':

                end_users_dict["users"].append(data)

                if len(end_users_dict["users"]) == 100:
                    end_user_payload.append(json.dumps(end_users_dict))
                    end_users_dict = {"users": []}

            elif role == 'agent' or role == 'admin':

                agent_users_dict["users"].append(data)

                if len(agent_users_dict["users"]) == 100:
                    agents_payload.append(json.dumps(agent_users_dict))
                    agent_users_dict = {"users": []}

    if end_users_dict["users"]:
        end_user_payload.append(json.dumps(end_users_dict))
    #
    if agent_users_dict["users"]:
        agents_payload.append(json.dumps(agent_users_dict))

    # # # iterate through the payloads list and post one payload at a time to Zendesk Support
    # for payload in end_user_payload:
    #     print(payload)
    #     r = session.post(URL, data=payload)
    #     result = r.json()
    #     print(result)
    # # #
    # for load in agents_payload:
    #     r = session.post("https://z3nplatformdevjg.zendesk.com/api/v2/users/create_or_update_many.json", data=load)
    #     result = r.json()
    #     print(result)
def read_csv_tickets(tickets_file):
    # This function reads the data from the csv file
    csvReader = csv.reader(tickets_file,newline='')
    data = list(csvReader)
    header = data[:1]

    return data

def read_csv(file):
    # This function reads the data from the csv file
    csvReader = csv.reader(file)
    data = list(csvReader)
    header = data[:1]

    return data


def main():
    # creates a requests session object and configures it with your authentication information.
    session = requests.Session()
    session.headers = {'Content-Type': 'application/json'}
    session.auth = 'garciajrjoseluis@gmail.com/token', 'c2hERX4JpJ6b8IZjtrCYTxkSegxThaOk1o8oJhSA'

    # Opening csv
    organizations_file = open('organizations.csv')
    comments_file = open('ticket_comments.csv')
    tickets_file = open('tickets.csv')
    users_file = open('users.csv')

    # Read the data from the csv file
    organizations_data = read_csv(organizations_file)
    comments_data = read_csv(comments_file)
    tickets_data = read_csv(tickets_file)
    users_data = read_csv(users_file)

    # organization map with org id and external id
    org_map = get_org_map(session)

    org_memberships = {}

    create_organizations(organizations_data, session, org_map)
    create_users(users_data, session, org_map, org_memberships)
    #
    user_map = get_user_map(session)
    create_org_memberships(session, org_memberships, user_map, org_map)
    comments = get_comments(comments_data)

    create_tickets(tickets_data, session, user_map, org_map,comments)


if __name__ == '__main__':
    main()
